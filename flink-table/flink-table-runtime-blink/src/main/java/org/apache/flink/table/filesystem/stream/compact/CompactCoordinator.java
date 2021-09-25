/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.filesystem.stream.compact;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.filesystem.stream.TaskTracker;
import org.apache.flink.table.filesystem.stream.compact.CompactMessages.*;
import org.apache.flink.table.runtime.util.BinPacking;
import org.apache.flink.util.function.SupplierWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.Function;

/**
 * This is the single (non-parallel) monitoring task which coordinate input files to compaction
 * units. - Receives in-flight input files inside checkpoint. - Receives all upstream end input
 * messages after the checkpoint completes successfully, starts coordination.
 *
 * <p>{@link CompactionUnit} and {@link EndCompaction} must be sent to the downstream in an orderly
 * manner, while {@link EndCompaction} is broadcast emitting, so unit and endCompaction use the
 * broadcast emitting mechanism together. Since unit is broadcast, we want it to be processed by a
 * single task, so we carry the ID in the unit and let the downstream task select its own unit.
 *
 * <p>NOTE: The coordination is a stable algorithm, which can ensure that the downstream can perform
 * compaction at any time without worrying about fail over.
 *
 * <p>STATE: This operator stores input files in state, after the checkpoint completes successfully,
 * input files are taken out from the state for coordination.
 */
public class CompactCoordinator extends AbstractStreamOperator<CoordinatorOutput>
        implements OneInputStreamOperator<CoordinatorInput, CoordinatorOutput> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(CompactCoordinator.class);

    private final SupplierWithException<FileSystem, IOException> fsFactory;
    private final long targetFileSize;

    private transient FileSystem fileSystem;
    private transient FileSystem compactFs;

    private transient ListState<Map<Long, Map<String, List<Path>>>> inputFilesState;
    private transient TreeMap<Long, Map<String, List<Path>>> inputFiles;
    private transient Map<String, List<Path>> currentInputFiles;

    private transient TaskTracker inputTaskTracker;

    private transient MapSerializer<Long, Map<String, List<Path>>> mapSerializer;

    private Path compactPath;

    public CompactCoordinator(
            SupplierWithException<FileSystem, IOException> fsFactory, long targetFileSize) {
        this.fsFactory = fsFactory;
        this.targetFileSize = targetFileSize;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        compactPath =
                new Path(
                        "hdfs://inner-di-hdfs.1sapp.com/warehouse/dw/flink_sql.db/"
                                + System.getProperty("jobName")
                                + "/"
                                + "compactionInfo.info");

        compactFs = FileSystem.get(compactPath.toUri());

        fileSystem = fsFactory.get();

        mapSerializer =
                new MapSerializer<>(
                        LongSerializer.INSTANCE,
                        new MapSerializer<>(
                                StringSerializer.INSTANCE,
                                new ListSerializer<>(
                                        new KryoSerializer<>(Path.class, getExecutionConfig()))));

        ListStateDescriptor<Map<Long, Map<String, List<Path>>>> filesDescriptor =
                new ListStateDescriptor<>("files-state", mapSerializer);
        inputFilesState = context.getOperatorStateStore().getListState(filesDescriptor);
        inputFiles = new TreeMap<>();
        currentInputFiles = new HashMap<>();
        if (context.isRestored()) {
            inputFiles.putAll(inputFilesState.get().iterator().next());
        } else {
            // 直接启动时，判断外部存储是否含有未压缩的文件，如果有则恢复到状态里
            if (compactFs.exists(compactPath)) {
                try (FSDataInputStream open = compactFs.open(compactPath)) {
                    byte[] bytes = open.readNBytes(Integer.MAX_VALUE);
                    DataInputDeserializer dataInputDeserializer = new DataInputDeserializer();
                    dataInputDeserializer.setBuffer(bytes);
                    Map<Long, Map<String, List<Path>>> restore =
                            mapSerializer.deserialize(dataInputDeserializer);
                    // checkpointId变为<=0，重启后 checkpoint id 从1开始
                    long i = 0;
                    Map<Long, Map<String, List<Path>>> paths = new HashMap<>();
                    for (Map v : restore.values()) {
                        paths.put(i--, v);
                    }
                    inputFiles.putAll(paths);
                    LOG.info(
                            "restore from hdfs successfully! the input files are : {}", inputFiles);
                } catch (Exception e) {
                    LOG.warn("can not read compaction info from hdfs path [{}].", compactPath);
                }
            }
        }
    }

    @Override
    public void processElement(StreamRecord<CoordinatorInput> element) throws Exception {
        CoordinatorInput value = element.getValue();
        if (value instanceof InputFile) {
            InputFile file = (InputFile) value;
            currentInputFiles
                    .computeIfAbsent(file.getPartition(), k -> new ArrayList<>())
                    .add(file.getFile());
        } else if (value instanceof EndCheckpoint) {
            EndCheckpoint endCheckpoint = (EndCheckpoint) value;
            if (inputTaskTracker == null) {
                inputTaskTracker = new TaskTracker(endCheckpoint.getNumberOfTasks());
            }

            // ensure all files are ready to be compacted.
            boolean triggerCommit =
                    inputTaskTracker.add(
                            endCheckpoint.getCheckpointId(), endCheckpoint.getTaskId());
            if (triggerCommit) {
                commitUpToCheckpoint(endCheckpoint.getCheckpointId());
            }
        } else {
            throw new UnsupportedOperationException("Unsupported input message: " + value);
        }
    }

    private void commitUpToCheckpoint(long checkpointId) {
        Map<Long, Map<String, List<Path>>> headMap = inputFiles.headMap(checkpointId, true);
        for (Map.Entry<Long, Map<String, List<Path>>> entry : headMap.entrySet()) {
            coordinate(entry.getKey(), entry.getValue());
        }
        headMap.clear();
    }

    /** Do stable compaction coordination. */
    private void coordinate(long checkpointId, Map<String, List<Path>> partFiles) {
        Function<Path, Long> sizeFunc =
                path -> {
                    try {
                        return fileSystem.getFileStatus(path).getLen();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                };

        // We need a stable compaction algorithm.
        Map<String, List<List<Path>>> compactUnits = new HashMap<>();
        partFiles.forEach(
                (p, files) -> {
                    // Sort files for stable compaction algorithm.
                    files.sort(Comparator.comparing(Path::getPath));
                    compactUnits.put(p, BinPacking.pack(files, sizeFunc, targetFileSize));
                });

        // Now, send this stable pack list to compactor.
        // NOTE, use broadcast emitting (Because it needs to emit checkpoint barrier),
        // operators will pick its units by unit id and task id.
        int unitId = 0;
        for (Map.Entry<String, List<List<Path>>> unitsEntry : compactUnits.entrySet()) {
            String partition = unitsEntry.getKey();
            for (List<Path> unit : unitsEntry.getValue()) {
                output.collect(new StreamRecord<>(new CompactionUnit(unitId, partition, unit)));
                unitId++;
            }
        }

        LOG.debug("Coordinate checkpoint-{}, compaction units are: {}", checkpointId, compactUnits);

        // Emit checkpoint barrier
        output.collect(new StreamRecord<>(new EndCompaction(checkpointId)));
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        inputFilesState.clear();
        inputFiles.put(context.getCheckpointId(), new HashMap<>(currentInputFiles));
        inputFilesState.add(inputFiles);
        // inputFiles需要写一份到外部存储
        try (FSDataOutputStream fsDataOutputStream =
                compactFs.create(compactPath, FileSystem.WriteMode.OVERWRITE)) {
            DataOutputSerializer outputSerializer = new DataOutputSerializer(1024);
            mapSerializer.serialize(inputFiles, outputSerializer);
            fsDataOutputStream.write(outputSerializer.getCopyOfBuffer());
            fsDataOutputStream.flush();
        } catch (Exception e) {
            LOG.warn("can not write compaction info to hdfs path [{}].", compactPath);
        }
        currentInputFiles.clear();
    }
}

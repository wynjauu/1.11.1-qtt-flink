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

package org.apache.flink.table.filesystem;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.*;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink.BucketsBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.filesystem.stream.PartitionCommitInfo;
import org.apache.flink.table.filesystem.stream.PartitionCommitter;
import org.apache.flink.table.filesystem.stream.StreamingFileWriter;
import org.apache.flink.table.filesystem.stream.StreamingSink;
import org.apache.flink.table.filesystem.stream.compact.CompactReader;
import org.apache.flink.table.filesystem.stream.compact.FileInputFormatCompactReader;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.OverwritableTableSink;
import org.apache.flink.table.sinks.PartitionableTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.PartitionPathUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.filesystem.FileSystemOptions.*;
import static org.apache.flink.table.filesystem.FileSystemTableFactory.createFormatFactory;
import static org.apache.flink.table.filesystem.stream.compact.CompactOperator.convertToUncompacted;

/** File system {@link TableSink}. */
public class FileSystemTableSink
        implements AppendStreamTableSink<RowData>, PartitionableTableSink, OverwritableTableSink {

    private final ObjectIdentifier tableIdentifier;
    private final boolean isBounded;
    private final TableSchema schema;
    private final List<String> partitionKeys;
    private final Path path;
    private final String defaultPartName;
    private final Map<String, String> properties;

    private boolean overwrite = false;
    private boolean dynamicGrouping = false;
    private LinkedHashMap<String, String> staticPartitions = new LinkedHashMap<>();
    private List<Map<String, String>> readPartitions;

    /**
     * Construct a file system table sink.
     *
     * @param isBounded whether the input of sink is bounded.
     * @param schema schema of the table.
     * @param path directory path of the file system table.
     * @param partitionKeys partition keys of the table.
     * @param defaultPartName The default partition name in case the dynamic partition column value
     *     is null/empty string.
     * @param properties properties.
     */
    public FileSystemTableSink(
            ObjectIdentifier tableIdentifier,
            boolean isBounded,
            TableSchema schema,
            Path path,
            List<String> partitionKeys,
            String defaultPartName,
            Map<String, String> properties) {
        this.tableIdentifier = tableIdentifier;
        this.isBounded = isBounded;
        this.schema = schema;
        this.path = path;
        this.defaultPartName = defaultPartName;
        this.partitionKeys = partitionKeys;
        this.properties = properties;
    }

    @Override
    public final DataStreamSink<?> consumeDataStream(DataStream<RowData> dataStream) {
        RowDataPartitionComputer computer =
                new RowDataPartitionComputer(
                        defaultPartName,
                        schema.getFieldNames(),
                        schema.getFieldDataTypes(),
                        partitionKeys.toArray(new String[0]));
        String randomPrefix = "part-" + UUID.randomUUID().toString();

        FileSystemFactory fsFactory = FileSystem::get;
        FileSystemWithUserFactory fsWithUserFactory = FileSystem::getWithUser;

        if (isBounded) {
            EmptyMetaStoreFactory metaStoreFactory = new EmptyMetaStoreFactory(path);
            OutputFileConfig outputFileConfig =
                    OutputFileConfig.builder().withPartPrefix(randomPrefix).build();
            FileSystemOutputFormat.Builder<RowData> builder =
                    new FileSystemOutputFormat.Builder<>();
            builder.setPartitionComputer(computer);
            builder.setDynamicGrouped(dynamicGrouping);
            builder.setPartitionColumns(partitionKeys.toArray(new String[0]));
            builder.setFormatFactory(createOutputFormatFactory());
            builder.setMetaStoreFactory(metaStoreFactory);
            builder.setFileSystemFactory(fsFactory);
            builder.setOverwrite(overwrite);
            builder.setStaticPartitions(staticPartitions);
            builder.setTempPath(toStagingPath());
            builder.setOutputFileConfig(outputFileConfig);
            return dataStream
                    .writeUsingOutputFormat(builder.build())
                    .setParallelism(dataStream.getParallelism());
        } else {
            Configuration conf = new Configuration();
            properties.forEach(conf::setString);
            boolean autoCompaction = conf.getBoolean(FileSystemOptions.AUTO_COMPACTION);
            Object writer = createWriter();
            boolean isEncoder = writer instanceof Encoder;
            TableBucketAssigner assigner = new TableBucketAssigner(computer);
            TableRollingPolicy rollingPolicy =
                    new TableRollingPolicy(
                            !isEncoder || autoCompaction,
                            conf.get(SINK_ROLLING_POLICY_FILE_SIZE).getBytes(),
                            conf.get(SINK_ROLLING_POLICY_ROLLOVER_INTERVAL).toMillis());

            OutputFileConfig.OutputFileConfigBuilder fileNamingBuilder = OutputFileConfig.builder();
            fileNamingBuilder =
                    autoCompaction
                            ? fileNamingBuilder.withPartPrefix(convertToUncompacted(randomPrefix))
                            : fileNamingBuilder.withPartPrefix(randomPrefix);
            OutputFileConfig fileNamingConfig = fileNamingBuilder.build();

            BucketsBuilder<RowData, String, ? extends BucketsBuilder<RowData, ?, ?>> bucketsBuilder;
            if (isEncoder) {
                //noinspection unchecked
                bucketsBuilder =
                        StreamingFileSink.forRowFormat(
                                        path,
                                        new ProjectionEncoder((Encoder<RowData>) writer, computer))
                                .withBucketAssigner(assigner)
                                .withOutputFileConfig(fileNamingConfig)
                                .withRollingPolicy(rollingPolicy);
            } else {
                //noinspection unchecked
                bucketsBuilder =
                        StreamingFileSink.forBulkFormat(
                                        path,
                                        new ProjectionBulkFactory(
                                                (BulkWriter.Factory<RowData>) writer, computer))
                                .withBucketAssigner(assigner)
                                .withOutputFileConfig(fileNamingConfig)
                                .withRollingPolicy(rollingPolicy);
            }

            long bucketCheckInterval = conf.get(SINK_ROLLING_POLICY_CHECK_INTERVAL).toMillis();

            DataStream<PartitionCommitInfo> writerStream;
            if (autoCompaction) {
                long compactionSize =
                        conf.getOptional(FileSystemOptions.COMPACTION_FILE_SIZE)
                                .orElse(conf.get(SINK_ROLLING_POLICY_FILE_SIZE))
                                .getBytes();

                CompactReader.Factory<RowData> reader =
                        FileInputFormatCompactReader.factory(
                                (FileInputFormat<RowData>) getInputFormat());

                writerStream =
                        StreamingSink.compactionWriter(
                                dataStream,
                                bucketCheckInterval,
                                bucketsBuilder,
                                fsFactory,
                                fsWithUserFactory,
                                path,
                                reader,
                                compactionSize,
                                conf);
            } else {
                writerStream =
                        StreamingSink.writer(dataStream, bucketCheckInterval, bucketsBuilder, conf);
            }

            return StreamingSink.sink(
                    writerStream,
                    path,
                    tableIdentifier,
                    partitionKeys,
                    new EmptyMetaStoreFactory(path),
                    fsFactory,
                    fsWithUserFactory,
                    conf);
        }
    }

    private InputFormat<RowData, ?> getInputFormat() {

        FileSystemFormatFactory formatFactory = createFormatFactory(properties);
        Configuration conf = new Configuration();
        properties.forEach(conf::setString);
        return formatFactory.createReader(
                new FileSystemFormatFactory.ReaderContext() {

                    @Override
                    public TableSchema getSchema() {
                        return schema;
                    }

                    @Override
                    public ReadableConfig getFormatOptions() {
                        return new DelegatingConfiguration(
                                conf, formatFactory.factoryIdentifier() + ".");
                    }

                    @Override
                    public List<String> getPartitionKeys() {
                        return partitionKeys;
                    }

                    @Override
                    public String getDefaultPartName() {
                        return defaultPartName;
                    }

                    @Override
                    public Path[] getPaths() {
                        if (partitionKeys.isEmpty()) {
                            return new Path[] {path};
                        } else {
                            return getOrFetchPartitions().stream()
                                    .map(FileSystemTableSink.this::toFullLinkedPartSpec)
                                    .map(PartitionPathUtils::generatePartitionPath)
                                    .map(n -> new Path(path, n))
                                    .toArray(Path[]::new);
                        }
                    }

                    @Override
                    public int[] getProjectFields() {
                        return readFields();
                    }

                    @Override
                    public long getPushedDownLimit() {
                        return Long.MAX_VALUE;
                    }

                    @Override
                    public List<Expression> getPushedDownFilters() {
                        return Collections.emptyList();
                    }
                });
    }

    private List<Map<String, String>> getOrFetchPartitions() {
        if (readPartitions == null) {
            readPartitions = getPartitions();
        }
        return readPartitions;
    }

    public List<Map<String, String>> getPartitions() {
        try {
            return PartitionPathUtils.searchPartSpecAndPaths(
                            path.getFileSystem(), path, partitionKeys.size())
                    .stream()
                    .map(tuple2 -> tuple2.f0)
                    .map(
                            spec -> {
                                LinkedHashMap<String, String> ret = new LinkedHashMap<>();
                                spec.forEach(
                                        (k, v) -> ret.put(k, defaultPartName.equals(v) ? null : v));
                                return ret;
                            })
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new TableException("Fetch partitions fail.", e);
        }
    }

    private int[] readFields() {
        return IntStream.range(0, schema.getFieldCount()).toArray();
    }

    private LinkedHashMap<String, String> toFullLinkedPartSpec(Map<String, String> part) {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        for (String k : partitionKeys) {
            if (!part.containsKey(k)) {
                throw new TableException(
                        "Partition keys are: "
                                + partitionKeys
                                + ", incomplete partition spec: "
                                + part);
            }
            map.put(k, part.get(k));
        }
        return map;
    }

    public static DataStreamSink<?> createStreamingSink(
            Configuration conf,
            Path path,
            List<String> partitionKeys,
            ObjectIdentifier tableIdentifier,
            boolean overwrite,
            DataStream<RowData> inputStream,
            BucketsBuilder<RowData, String, ? extends BucketsBuilder<RowData, ?, ?>> bucketsBuilder,
            TableMetaStoreFactory msFactory,
            FileSystemFactory fsFactory,
            FileSystemWithUserFactory fsWithUserFactory,
            long rollingCheckInterval) {
        if (overwrite) {
            throw new IllegalStateException("Streaming mode not support overwrite.");
        }

        StreamingFileWriter fileWriter =
                new StreamingFileWriter(rollingCheckInterval, bucketsBuilder);

        // set user specify write hdfs user
        String hadoopUser = conf.getString(HADOOP_USER);
        fileWriter.setHadoopUser(hadoopUser);

        int userSpecifyParallelism = conf.getInteger(PARALLELISM);
        int actualParallelism =
                userSpecifyParallelism == 1 ? inputStream.getParallelism() : userSpecifyParallelism;

        DataStream<PartitionCommitInfo> writerStream =
                inputStream
                        .transform(
                                StreamingFileWriter.class.getSimpleName(),
                                TypeExtractor.createTypeInfo(PartitionCommitInfo.class),
                                fileWriter)
                        .setParallelism(actualParallelism);

        DataStream<?> returnStream = writerStream;

        // save committer when we don't need it.
        if (partitionKeys.size() > 0 && conf.contains(SINK_PARTITION_COMMIT_POLICY_KIND)) {
            PartitionCommitter committer =
                    new PartitionCommitter(
                            path,
                            tableIdentifier,
                            partitionKeys,
                            msFactory,
                            fsFactory,
                            fsWithUserFactory,
                            conf);
            returnStream =
                    writerStream
                            .transform(
                                    PartitionCommitter.class.getSimpleName(), Types.VOID, committer)
                            .setParallelism(1)
                            .setMaxParallelism(1);
        }
        //noinspection unchecked
        return returnStream.addSink(new DiscardingSink()).setParallelism(1);
    }

    public static DataStreamSink<?> createStreamingSink(
            Configuration conf,
            Path path,
            List<String> partitionKeys,
            ObjectIdentifier tableIdentifier,
            boolean overwrite,
            DataStream<RowData> inputStream,
            BucketsBuilder<RowData, String, ? extends BucketsBuilder<RowData, ?, ?>> bucketsBuilder,
            TableMetaStoreFactory msFactory,
            FileSystemFactory fsFactory,
            long rollingCheckInterval) {
        return createStreamingSink(
                conf,
                path,
                partitionKeys,
                tableIdentifier,
                overwrite,
                inputStream,
                bucketsBuilder,
                msFactory,
                fsFactory,
                null,
                rollingCheckInterval);
    }

    private Path toStagingPath() {
        Path stagingDir = new Path(path, ".staging_" + System.currentTimeMillis());
        try {
            FileSystem fs = stagingDir.getFileSystem();
            Preconditions.checkState(
                    fs.exists(stagingDir) || fs.mkdirs(stagingDir),
                    "Failed to create staging dir " + stagingDir);
            return stagingDir;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private OutputFormatFactory<RowData> createOutputFormatFactory() {
        Object writer = createWriter();
        return writer instanceof Encoder
                ? path -> createEncoderOutputFormat((Encoder<RowData>) writer, path)
                : path -> createBulkWriterOutputFormat((BulkWriter.Factory<RowData>) writer, path);
    }

    private Object createWriter() {
        FileSystemFormatFactory formatFactory = createFormatFactory(properties);
        Configuration conf = new Configuration();
        properties.forEach(conf::setString);

        FileSystemFormatFactory.WriterContext context =
                new FileSystemFormatFactory.WriterContext() {

                    @Override
                    public TableSchema getSchema() {
                        return schema;
                    }

                    @Override
                    public ReadableConfig getFormatOptions() {
                        return new DelegatingConfiguration(
                                conf, formatFactory.factoryIdentifier() + ".");
                    }

                    @Override
                    public List<String> getPartitionKeys() {
                        return partitionKeys;
                    }
                };

        Optional<Encoder<RowData>> encoder = formatFactory.createEncoder(context);
        Optional<BulkWriter.Factory<RowData>> bulk = formatFactory.createBulkWriterFactory(context);

        if (encoder.isPresent()) {
            return encoder.get();
        } else if (bulk.isPresent()) {
            return bulk.get();
        } else {
            throw new TableException(
                    formatFactory + " format should implement at least one Encoder or BulkWriter");
        }
    }

    private static OutputFormat<RowData> createBulkWriterOutputFormat(
            BulkWriter.Factory<RowData> factory, Path path) {
        return new OutputFormat<RowData>() {

            private static final long serialVersionUID = 1L;

            private transient BulkWriter<RowData> writer;

            @Override
            public void configure(Configuration parameters) {}

            @Override
            public void open(int taskNumber, int numTasks) throws IOException {
                this.writer =
                        factory.create(
                                path.getFileSystem().create(path, FileSystem.WriteMode.OVERWRITE));
            }

            @Override
            public void writeRecord(RowData record) throws IOException {
                writer.addElement(record);
            }

            @Override
            public void close() throws IOException {
                writer.flush();
                writer.finish();
            }
        };
    }

    private static OutputFormat<RowData> createEncoderOutputFormat(
            Encoder<RowData> encoder, Path path) {
        return new OutputFormat<RowData>() {

            private static final long serialVersionUID = 1L;

            private transient FSDataOutputStream output;

            @Override
            public void configure(Configuration parameters) {}

            @Override
            public void open(int taskNumber, int numTasks) throws IOException {
                this.output = path.getFileSystem().create(path, FileSystem.WriteMode.OVERWRITE);
            }

            @Override
            public void writeRecord(RowData record) throws IOException {
                encoder.encode(record, output);
            }

            @Override
            public void close() throws IOException {
                this.output.flush();
                this.output.close();
            }
        };
    }

    @Override
    public FileSystemTableSink configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return this;
    }

    @Override
    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    @Override
    public void setStaticPartition(Map<String, String> partitions) {
        this.staticPartitions = toPartialLinkedPartSpec(partitions);
    }

    private LinkedHashMap<String, String> toPartialLinkedPartSpec(Map<String, String> part) {
        LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
        for (String partitionKey : partitionKeys) {
            if (part.containsKey(partitionKey)) {
                partSpec.put(partitionKey, part.get(partitionKey));
            }
        }
        return partSpec;
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public DataType getConsumedDataType() {
        return schema.toRowDataType().bridgedTo(RowData.class);
    }

    @Override
    public boolean configurePartitionGrouping(boolean supportsGrouping) {
        this.dynamicGrouping = supportsGrouping;
        return dynamicGrouping;
    }

    /** Table bucket assigner, wrap {@link PartitionComputer}. */
    public static class TableBucketAssigner implements BucketAssigner<RowData, String> {

        private final PartitionComputer<RowData> computer;

        public TableBucketAssigner(PartitionComputer<RowData> computer) {
            this.computer = computer;
        }

        @Override
        public String getBucketId(RowData element, Context context) {
            try {
                return PartitionPathUtils.generatePartitionPath(
                        computer.generatePartValues(element));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }

    /** Table {@link RollingPolicy}, it extends {@link CheckpointRollingPolicy} for bulk writers. */
    public static class TableRollingPolicy extends CheckpointRollingPolicy<RowData, String> {

        private final boolean rollOnCheckpoint;
        private final long rollingFileSize;
        private final long rollingTimeInterval;

        public TableRollingPolicy(
                boolean rollOnCheckpoint, long rollingFileSize, long rollingTimeInterval) {
            this.rollOnCheckpoint = rollOnCheckpoint;
            Preconditions.checkArgument(rollingFileSize > 0L);
            Preconditions.checkArgument(rollingTimeInterval > 0L);
            this.rollingFileSize = rollingFileSize;
            this.rollingTimeInterval = rollingTimeInterval;
        }

        @Override
        public boolean shouldRollOnCheckpoint(PartFileInfo<String> partFileState) {
            try {
                return rollOnCheckpoint || partFileState.getSize() > rollingFileSize;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean shouldRollOnEvent(PartFileInfo<String> partFileState, RowData element)
                throws IOException {
            return partFileState.getSize() > rollingFileSize;
        }

        @Override
        public boolean shouldRollOnProcessingTime(
                PartFileInfo<String> partFileState, long currentTime) {
            return currentTime - partFileState.getCreationTime() >= rollingTimeInterval;
        }
    }

    private static class ProjectionEncoder implements Encoder<RowData> {

        private final Encoder<RowData> encoder;
        private final RowDataPartitionComputer computer;

        private ProjectionEncoder(Encoder<RowData> encoder, RowDataPartitionComputer computer) {
            this.encoder = encoder;
            this.computer = computer;
        }

        @Override
        public void encode(RowData element, OutputStream stream) throws IOException {
            encoder.encode(computer.projectColumnsToWrite(element), stream);
        }
    }

    /** Project row to non-partition fields. */
    public static class ProjectionBulkFactory implements BulkWriter.Factory<RowData> {

        private final BulkWriter.Factory<RowData> factory;
        private final RowDataPartitionComputer computer;

        public ProjectionBulkFactory(
                BulkWriter.Factory<RowData> factory, RowDataPartitionComputer computer) {
            this.factory = factory;
            this.computer = computer;
        }

        @Override
        public BulkWriter<RowData> create(FSDataOutputStream out) throws IOException {
            BulkWriter<RowData> writer = factory.create(out);
            return new BulkWriter<RowData>() {

                @Override
                public void addElement(RowData element) throws IOException {
                    writer.addElement(computer.projectColumnsToWrite(element));
                }

                @Override
                public void flush() throws IOException {
                    writer.flush();
                }

                @Override
                public void finish() throws IOException {
                    writer.finish();
                }
            };
        }
    }
}

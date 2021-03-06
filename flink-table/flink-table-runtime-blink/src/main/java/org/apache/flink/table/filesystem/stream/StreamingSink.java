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

package org.apache.flink.table.filesystem.stream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.filesystem.FileSystemFactory;
import org.apache.flink.table.filesystem.FileSystemWithUserFactory;
import org.apache.flink.table.filesystem.TableMetaStoreFactory;
import org.apache.flink.table.filesystem.stream.compact.*;
import org.apache.flink.table.filesystem.stream.compact.CompactMessages.CoordinatorInput;
import org.apache.flink.table.filesystem.stream.compact.CompactMessages.CoordinatorOutput;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.filesystem.FileSystemOptions.*;

/** Helper for creating streaming file sink. */
public class StreamingSink {
    private StreamingSink() {}

    /**
     * Create a file writer by input stream. This is similar to {@link StreamingFileSink}, in
     * addition, it can emit {@link PartitionCommitInfo} to down stream.
     */
    public static <T> DataStream<PartitionCommitInfo> writer(
            DataStream<T> inputStream,
            long bucketCheckInterval,
            StreamingFileSink.BucketsBuilder<
                            T, String, ? extends StreamingFileSink.BucketsBuilder<T, String, ?>>
                    bucketsBuilder,
            Configuration conf) {
        StreamingFileWriter<T> fileWriter =
                new StreamingFileWriter<>(bucketCheckInterval, bucketsBuilder);
        return inputStream
                .transform(
                        StreamingFileWriter.class.getSimpleName(),
                        TypeInformation.of(PartitionCommitInfo.class),
                        fileWriter)
                .setParallelism(conf.getOptional(PARALLELISM).orElse(inputStream.getParallelism()));
    }

    /**
     * Create a file writer with compaction operators by input stream. In addition, it can emit
     * {@link PartitionCommitInfo} to down stream.
     */
    public static <T> DataStream<PartitionCommitInfo> compactionWriter(
            DataStream<T> inputStream,
            long bucketCheckInterval,
            StreamingFileSink.BucketsBuilder<
                            T, String, ? extends StreamingFileSink.BucketsBuilder<T, String, ?>>
                    bucketsBuilder,
            FileSystemFactory fsFactory,
            FileSystemWithUserFactory fsWithUserFactory,
            Path path,
            CompactReader.Factory<T> readFactory,
            long targetFileSize,
            Configuration conf) {
        CompactFileWriter<T> writer = new CompactFileWriter<>(bucketCheckInterval, bucketsBuilder);

        SupplierWithException<FileSystem, IOException> fsSupplier =
                (SupplierWithException<FileSystem, IOException> & Serializable)
                        () -> {
                            String user = conf.getString(HADOOP_USER);
                            return StringUtils.isNullOrWhitespaceOnly(user)
                                    ? fsFactory.create(path.toUri())
                                    : fsWithUserFactory.create(path.toUri(), user);
                        };

        CompactCoordinator coordinator = new CompactCoordinator(fsSupplier, targetFileSize);

        final Optional<Integer> parallelism = conf.getOptional(PARALLELISM);

        SingleOutputStreamOperator<CoordinatorOutput> coordinatorOp =
                inputStream
                        .transform(
                                "streaming-writer",
                                TypeInformation.of(CoordinatorInput.class),
                                writer)
                        .setParallelism(parallelism.orElse(inputStream.getParallelism()))
                        .transform(
                                "compact-coordinator",
                                TypeInformation.of(CoordinatorOutput.class),
                                coordinator)
                        .setParallelism(1)
                        .setMaxParallelism(1);

        CompactWriter.Factory<T> writerFactory =
                CompactBucketWriter.factory(
                        (SupplierWithException<BucketWriter<T, String>, IOException> & Serializable)
                                bucketsBuilder::createBucketWriter);

        CompactOperator<T> compacter =
                new CompactOperator<>(fsSupplier, readFactory, writerFactory);

        return coordinatorOp
                .broadcast()
                .transform(
                        "compact-operator",
                        TypeInformation.of(PartitionCommitInfo.class),
                        compacter)
                .setParallelism(parallelism.orElse(inputStream.getParallelism()));
    }

    /**
     * Create a sink from file writer. Decide whether to add the node to commit partitions according
     * to options.
     */
    public static DataStreamSink<?> sink(
            DataStream<PartitionCommitInfo> writer,
            Path locationPath,
            ObjectIdentifier identifier,
            List<String> partitionKeys,
            TableMetaStoreFactory msFactory,
            FileSystemFactory fsFactory,
            FileSystemWithUserFactory fsWithUserFactory,
            Configuration options) {
        DataStream<?> stream = writer;
        if (partitionKeys.size() > 0 && options.contains(SINK_PARTITION_COMMIT_POLICY_KIND)) {
            PartitionCommitter committer =
                    new PartitionCommitter(
                            locationPath,
                            identifier,
                            partitionKeys,
                            msFactory,
                            fsFactory,
                            fsWithUserFactory,
                            options);
            stream =
                    writer.transform(
                                    PartitionCommitter.class.getSimpleName(), Types.VOID, committer)
                            .setParallelism(1)
                            .setMaxParallelism(1);
        }

        return stream.addSink(new DiscardingSink<>()).name("end").setParallelism(1);
    }
}

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

package org.apache.flink.connectors.hive;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connectors.hive.write.HiveBulkWriterFactory;
import org.apache.flink.connectors.hive.write.HiveOutputFormatFactory;
import org.apache.flink.connectors.hive.write.HiveWriterFactory;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.orc.OrcSplitReaderUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.HadoopPathBasedBulkFormatBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink.BucketsBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.filesystem.FileSystemOptions;
import org.apache.flink.table.filesystem.FileSystemOutputFormat;
import org.apache.flink.table.filesystem.FileSystemTableSink;
import org.apache.flink.table.filesystem.FileSystemTableSink.TableBucketAssigner;
import org.apache.flink.table.filesystem.stream.PartitionCommitInfo;
import org.apache.flink.table.filesystem.stream.StreamingSink;
import org.apache.flink.table.filesystem.stream.compact.FileInputFormatCompactReader;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.OverwritableTableSink;
import org.apache.flink.table.sinks.PartitionableTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.PartitionPathUtils;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.orc.TypeDescription;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.filesystem.FileSystemOptions.*;
import static org.apache.flink.table.filesystem.stream.compact.CompactOperator.convertToUncompacted;

/** Table sink to write to Hive tables. */
public class HiveTableSink
        implements AppendStreamTableSink, PartitionableTableSink, OverwritableTableSink {

    private static final Logger LOG = LoggerFactory.getLogger(HiveTableSink.class);

    private final ReadableConfig flinkConf;
    private final boolean isBounded;
    private final JobConf jobConf;
    private final CatalogTable catalogTable;
    private final ObjectIdentifier identifier;
    private final TableSchema tableSchema;
    private final String hiveVersion;
    private final HiveShim hiveShim;

    private LinkedHashMap<String, String> staticPartitionSpec = new LinkedHashMap<>();

    private boolean overwrite = false;
    private boolean dynamicGrouping = false;

    public HiveTableSink(
            ReadableConfig flinkConf,
            boolean isBounded,
            JobConf jobConf,
            ObjectIdentifier identifier,
            CatalogTable table) {
        this.flinkConf = flinkConf;
        this.isBounded = isBounded;
        this.jobConf = jobConf;
        this.identifier = identifier;
        this.catalogTable = table;
        hiveVersion =
                Preconditions.checkNotNull(
                        jobConf.get(HiveCatalogValidator.CATALOG_HIVE_VERSION),
                        "Hive version is not defined");
        hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
        tableSchema = TableSchemaUtils.getPhysicalSchema(table.getSchema());
    }

    @Override
    public final DataStreamSink consumeDataStream(DataStream dataStream) {
        String[] partitionColumns = getPartitionKeys().toArray(new String[0]);
        String dbName = identifier.getDatabaseName();
        String tableName = identifier.getObjectName();
        try (HiveMetastoreClientWrapper client =
                HiveMetastoreClientFactory.create(
                        new HiveConf(jobConf, HiveConf.class), hiveVersion)) {
            Table table = client.getTable(dbName, tableName);
            StorageDescriptor sd = table.getSd();
            HiveTableMetaStoreFactory msFactory =
                    new HiveTableMetaStoreFactory(jobConf, hiveVersion, dbName, tableName);
            HadoopFileSystemFactory fsFactory = new HadoopFileSystemFactory(jobConf);

            Class hiveOutputFormatClz =
                    hiveShim.getHiveOutputFormatClass(Class.forName(sd.getOutputFormat()));
            boolean isCompressed =
                    jobConf.getBoolean(HiveConf.ConfVars.COMPRESSRESULT.varname, false);
            HiveWriterFactory writerFactory =
                    new HiveWriterFactory(
                            jobConf,
                            hiveOutputFormatClz,
                            sd.getSerdeInfo(),
                            tableSchema,
                            partitionColumns,
                            HiveReflectionUtils.getTableMetadata(hiveShim, table),
                            hiveShim,
                            isCompressed);
            String extension =
                    Utilities.getFileExtension(
                            jobConf,
                            isCompressed,
                            (HiveOutputFormat<?, ?>) hiveOutputFormatClz.newInstance());
            OutputFileConfig.OutputFileConfigBuilder fileNamingBuilder =
                    OutputFileConfig.builder()
                            .withPartPrefix("part-" + UUID.randomUUID().toString())
                            .withPartSuffix(extension == null ? "" : extension);

            if (isBounded) {
                FileSystemOutputFormat.Builder<Row> builder =
                        new FileSystemOutputFormat.Builder<>();
                builder.setPartitionComputer(
                        new HiveRowPartitionComputer(
                                hiveShim,
                                defaultPartName(),
                                tableSchema.getFieldNames(),
                                tableSchema.getFieldDataTypes(),
                                partitionColumns));
                builder.setDynamicGrouped(dynamicGrouping);
                builder.setPartitionColumns(partitionColumns);
                builder.setFileSystemFactory(fsFactory);
                builder.setFormatFactory(new HiveOutputFormatFactory(writerFactory));
                builder.setMetaStoreFactory(msFactory);
                builder.setOverwrite(overwrite);
                builder.setStaticPartitions(staticPartitionSpec);
                builder.setTempPath(
                        new org.apache.flink.core.fs.Path(toStagingDir(sd.getLocation(), jobConf)));
                builder.setOutputFileConfig(fileNamingBuilder.build());
                return dataStream
                        .writeUsingOutputFormat(builder.build())
                        .setParallelism(dataStream.getParallelism());
            } else {
                org.apache.flink.configuration.Configuration conf =
                        new org.apache.flink.configuration.Configuration();
                catalogTable.getOptions().forEach(conf::setString);
                HiveRowDataPartitionComputer partComputer =
                        new HiveRowDataPartitionComputer(
                                hiveShim,
                                defaultPartName(),
                                tableSchema.getFieldNames(),
                                tableSchema.getFieldDataTypes(),
                                partitionColumns);
                TableBucketAssigner assigner = new TableBucketAssigner(partComputer);
                HiveRollingPolicy rollingPolicy =
                        new HiveRollingPolicy(
                                conf.get(SINK_ROLLING_POLICY_FILE_SIZE).getBytes(),
                                conf.get(SINK_ROLLING_POLICY_ROLLOVER_INTERVAL).toMillis());

                boolean autoCompaction = conf.getBoolean(FileSystemOptions.AUTO_COMPACTION);
                if (autoCompaction) {
                    fileNamingBuilder.withPartPrefix(
                            convertToUncompacted(fileNamingBuilder.build().getPartPrefix()));
                }

                OutputFileConfig outputFileConfig = fileNamingBuilder.build();
                org.apache.flink.core.fs.Path path =
                        new org.apache.flink.core.fs.Path(sd.getLocation());
                BucketsBuilder<RowData, String, ? extends BucketsBuilder<RowData, ?, ?>> builder;
                if (flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_WRITER)) {
                    builder =
                            bucketsBuilderForMRWriter(
                                    writerFactory, sd, assigner, rollingPolicy, outputFileConfig);
                    LOG.info("Hive streaming sink: Use MapReduce RecordWriter writer.");
                } else {
                    Optional<BulkWriter.Factory<RowData>> bulkFactory =
                            createBulkWriterFactory(partitionColumns, sd);
                    if (bulkFactory.isPresent()) {
                        builder =
                                StreamingFileSink.forBulkFormat(
                                                path,
                                                new FileSystemTableSink.ProjectionBulkFactory(
                                                        bulkFactory.get(), partComputer))
                                        .withBucketAssigner(assigner)
                                        .withRollingPolicy(rollingPolicy)
                                        .withOutputFileConfig(outputFileConfig);
                        LOG.info("Hive streaming sink: Use native parquet&orc writer.");
                    } else {
                        builder =
                                bucketsBuilderForMRWriter(
                                        writerFactory,
                                        sd,
                                        assigner,
                                        rollingPolicy,
                                        outputFileConfig);
                        LOG.info(
                                "Hive streaming sink: Use MapReduce RecordWriter writer because BulkWriter Factory not available.");
                    }
                }
                long bucketCheckInterval = conf.get(SINK_ROLLING_POLICY_CHECK_INTERVAL).toMillis();

                DataStream<PartitionCommitInfo> writerStream;
                if (autoCompaction) {
                    long compactionSize =
                            conf.getOptional(FileSystemOptions.COMPACTION_FILE_SIZE)
                                    .orElse(conf.get(SINK_ROLLING_POLICY_FILE_SIZE))
                                    .getBytes();

                    writerStream =
                            StreamingSink.compactionWriter(
                                    dataStream,
                                    bucketCheckInterval,
                                    builder,
                                    fsFactory,
                                    null,
                                    path,
                                    FileInputFormatCompactReader.factory(
                                            (FileInputFormat<RowData>) getInputFormat(path)),
                                    compactionSize,
                                    conf);
                } else {
                    writerStream =
                            StreamingSink.writer(dataStream, bucketCheckInterval, builder, conf);
                }
                return StreamingSink.sink(
                        writerStream,
                        path,
                        identifier,
                        getPartitionKeys(),
                        msFactory,
                        fsFactory,
                        null,
                        conf);
            }
        } catch (TException e) {
            throw new CatalogException("Failed to query Hive metaStore", e);
        } catch (IOException e) {
            throw new FlinkRuntimeException("Failed to create staging dir", e);
        } catch (ClassNotFoundException e) {
            throw new FlinkHiveException("Failed to get output format class", e);
        } catch (IllegalAccessException | InstantiationException e) {
            throw new FlinkHiveException("Failed to instantiate output format instance", e);
        }
    }

    private InputFormat<RowData, ?> getInputFormat(org.apache.flink.core.fs.Path path) {
        org.apache.flink.configuration.Configuration conf =
                new org.apache.flink.configuration.Configuration();
        catalogTable.getOptions().forEach(conf::setString);
        String format = conf.getString(FactoryUtil.FORMAT);
        if (format == null) {
            throw new ValidationException(
                    String.format(
                            "Table options do not contain an option key '%s' for discovering a format.",
                            FORMAT));
        }
        List<String> partitionKeys = getPartitionKeys();
        FileSystemFormatFactory formatFactory =
                FactoryUtil.discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        FileSystemFormatFactory.class,
                        format);
        return formatFactory.createReader(
                new FileSystemFormatFactory.ReaderContext() {

                    @Override
                    public TableSchema getSchema() {
                        return tableSchema;
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
                        return PARTITION_DEFAULT_NAME.defaultValue();
                    }

                    @Override
                    public org.apache.flink.core.fs.Path[] getPaths() {
                        //                        if (getPartitionKeys().isEmpty()) {
                        //                            return new org.apache.flink.core.fs.Path[]
                        // {path};
                        //                        } else {
                        //                            return getPartitions(path,
                        // partitionKeys).stream()
                        //                                    .map(map -> toFullLinkedPartSpec(map,
                        // partitionKeys))
                        //
                        // .map(PartitionPathUtils::generatePartitionPath)
                        //                                    .map(n -> new
                        // org.apache.flink.core.fs.Path(path, n))
                        //
                        // .toArray(org.apache.flink.core.fs.Path[]::new);
                        //                        }
                        return new org.apache.flink.core.fs.Path[] {path};
                    }

                    @Override
                    public int[] getProjectFields() {
                        return IntStream.range(0, tableSchema.getFieldCount()).toArray();
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

    public List<Map<String, String>> getPartitions(
            org.apache.flink.core.fs.Path path, List<String> partitionKeys) {
        try {
            return PartitionPathUtils.searchPartSpecAndPaths(
                            path.getFileSystem(), path, partitionKeys.size())
                    .stream()
                    .map(tuple2 -> tuple2.f0)
                    .map(
                            spec -> {
                                LinkedHashMap<String, String> ret = new LinkedHashMap<>();
                                spec.forEach(
                                        (k, v) ->
                                                ret.put(
                                                        k,
                                                        PARTITION_DEFAULT_NAME
                                                                        .defaultValue()
                                                                        .equals(v)
                                                                ? null
                                                                : v));
                                return ret;
                            })
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new TableException("Fetch partitions fail.", e);
        }
    }

    private LinkedHashMap<String, String> toFullLinkedPartSpec(
            Map<String, String> part, List<String> partitionKeys) {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        for (String k : partitionKeys) {
            if (!part.containsKey(k)) {
                LOG.warn(
                        "Partition keys are: "
                                + partitionKeys
                                + ", incomplete partition spec: "
                                + part);
                continue;
            }
            map.put(k, part.get(k));
        }
        return map;
    }

    private BucketsBuilder<RowData, String, ? extends BucketsBuilder<RowData, ?, ?>>
            bucketsBuilderForMRWriter(
                    HiveWriterFactory writerFactory,
                    StorageDescriptor sd,
                    TableBucketAssigner assigner,
                    HiveRollingPolicy rollingPolicy,
                    OutputFileConfig outputFileConfig) {
        HiveBulkWriterFactory hadoopBulkFactory = new HiveBulkWriterFactory(writerFactory);
        return new HadoopPathBasedBulkFormatBuilder<>(
                        new Path(sd.getLocation()), hadoopBulkFactory, jobConf, assigner)
                .withRollingPolicy(rollingPolicy)
                .withOutputFileConfig(outputFileConfig);
    }

    private String defaultPartName() {
        return jobConf.get(
                HiveConf.ConfVars.DEFAULTPARTITIONNAME.varname,
                HiveConf.ConfVars.DEFAULTPARTITIONNAME.defaultStrVal);
    }

    private Optional<BulkWriter.Factory<RowData>> createBulkWriterFactory(
            String[] partitionColumns, StorageDescriptor sd) {
        String serLib = sd.getSerdeInfo().getSerializationLib().toLowerCase();
        int formatFieldCount = tableSchema.getFieldCount() - partitionColumns.length;
        String[] formatNames = new String[formatFieldCount];
        LogicalType[] formatTypes = new LogicalType[formatFieldCount];
        for (int i = 0; i < formatFieldCount; i++) {
            formatNames[i] = tableSchema.getFieldName(i).get();
            formatTypes[i] = tableSchema.getFieldDataType(i).get().getLogicalType();
        }
        RowType formatType = RowType.of(formatTypes, formatNames);
        Configuration formatConf = new Configuration(jobConf);
        sd.getSerdeInfo().getParameters().forEach(formatConf::set);
        if (serLib.contains("parquet")) {
            return Optional.of(
                    ParquetRowDataBuilder.createWriterFactory(
                            formatType, formatConf, hiveVersion.startsWith("3.")));
        } else if (serLib.contains("orc")) {
            TypeDescription typeDescription = OrcSplitReaderUtil.logicalTypeToOrcType(formatType);
            return Optional.of(
                    hiveShim.createOrcBulkWriterFactory(
                            formatConf, typeDescription.toString(), formatTypes));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public DataType getConsumedDataType() {
        DataType dataType = getTableSchema().toRowDataType();
        return isBounded ? dataType : dataType.bridgedTo(RowData.class);
    }

    @Override
    public TableSchema getTableSchema() {
        return tableSchema;
    }

    @Override
    public TableSink configure(String[] fieldNames, TypeInformation[] fieldTypes) {
        return this;
    }

    @Override
    public boolean configurePartitionGrouping(boolean supportsGrouping) {
        this.dynamicGrouping = supportsGrouping;
        return supportsGrouping;
    }

    // get a staging dir associated with a final dir
    private String toStagingDir(String finalDir, Configuration conf) throws IOException {
        String res = finalDir;
        if (!finalDir.endsWith(Path.SEPARATOR)) {
            res += Path.SEPARATOR;
        }
        // TODO: may append something more meaningful than a timestamp, like query ID
        res += ".staging_" + System.currentTimeMillis();
        Path path = new Path(res);
        FileSystem fs = path.getFileSystem(conf);
        Preconditions.checkState(
                fs.exists(path) || fs.mkdirs(path), "Failed to create staging dir " + path);
        fs.deleteOnExit(path);
        return res;
    }

    private List<String> getPartitionKeys() {
        return catalogTable.getPartitionKeys();
    }

    @Override
    public void setStaticPartition(Map<String, String> partitionSpec) {
        // make it a LinkedHashMap to maintain partition column order
        staticPartitionSpec = new LinkedHashMap<>();
        for (String partitionCol : getPartitionKeys()) {
            if (partitionSpec.containsKey(partitionCol)) {
                staticPartitionSpec.put(partitionCol, partitionSpec.get(partitionCol));
            }
        }
    }

    @Override
    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    /**
     * Getting size of the file is too expensive. See {@link HiveBulkWriterFactory#create}. We can't
     * check for every element, which will cause great pressure on DFS. Therefore, in this
     * implementation, only check the file size in {@link #shouldRollOnProcessingTime}, which can
     * effectively avoid DFS pressure.
     */
    private static class HiveRollingPolicy extends CheckpointRollingPolicy<RowData, String> {

        private final long rollingFileSize;
        private final long rollingTimeInterval;

        private HiveRollingPolicy(long rollingFileSize, long rollingTimeInterval) {
            Preconditions.checkArgument(rollingFileSize > 0L);
            Preconditions.checkArgument(rollingTimeInterval > 0L);
            this.rollingFileSize = rollingFileSize;
            this.rollingTimeInterval = rollingTimeInterval;
        }

        @Override
        public boolean shouldRollOnCheckpoint(PartFileInfo<String> partFileState) {
            return true;
        }

        @Override
        public boolean shouldRollOnEvent(PartFileInfo<String> partFileState, RowData element) {
            return false;
        }

        @Override
        public boolean shouldRollOnProcessingTime(
                PartFileInfo<String> partFileState, long currentTime) {
            try {
                return currentTime - partFileState.getCreationTime() >= rollingTimeInterval
                        || partFileState.getSize() > rollingFileSize;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}

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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.KafkaValidator;
import org.apache.flink.table.sources.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A version-agnostic Kafka {@link StreamTableSource}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and override {@link
 * #createKafkaConsumer(String, Properties, DeserializationSchema)}}.
 */
@Internal
public abstract class KafkaTableSourceBase
        implements StreamTableSource<Row>,
                DefinedProctimeAttribute,
                DefinedRowtimeAttributes,
                DefinedFieldMapping {

    // common table source attributes

    /** The schema of the table. */
    private final TableSchema schema;

    /** Field name of the processing time attribute, null if no processing time field is defined. */
    private final Optional<String> proctimeAttribute;

    /** Descriptor for a rowtime attribute. */
    private final List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors;

    /** Mapping for the fields of the table schema to fields of the physical returned type. */
    private final Optional<Map<String, String>> fieldMapping;

    // Kafka-specific attributes

    /** The Kafka topic to consume. */
    private final String topic;

    /** Properties for the Kafka consumer. */
    private final Properties properties;

    /** Deserialization schema for decoding records from Kafka. */
    private final DeserializationSchema<Row> deserializationSchema;

    /**
     * The startup mode for the contained consumer (default is {@link StartupMode#GROUP_OFFSETS}).
     */
    private final StartupMode startupMode;

    /**
     * Specific startup offsets; only relevant when startup mode is {@link
     * StartupMode#SPECIFIC_OFFSETS}.
     */
    private final Map<KafkaTopicPartition, Long> specificStartupOffsets;

    /**
     * The start timestamp to locate partition offsets; only relevant when startup mode is {@link
     * StartupMode#TIMESTAMP}.
     */
    private final long startupTimestampMillis;

    /** The default value when startup timestamp is not used. */
    private static final long DEFAULT_STARTUP_TIMESTAMP_MILLIS = 0L;

    /**
     * Creates a generic Kafka {@link StreamTableSource}.
     *
     * @param schema Schema of the produced table.
     * @param proctimeAttribute Field name of the processing time attribute.
     * @param rowtimeAttributeDescriptors Descriptor for a rowtime attribute
     * @param fieldMapping Mapping for the fields of the table schema to fields of the physical
     *     returned type.
     * @param topic Kafka topic to consume.
     * @param properties Properties for the Kafka consumer.
     * @param deserializationSchema Deserialization schema for decoding records from Kafka.
     * @param startupMode Startup mode for the contained consumer.
     * @param specificStartupOffsets Specific startup offsets; only relevant when startup mode is
     *     {@link StartupMode#SPECIFIC_OFFSETS}.
     * @param startupTimestampMillis Startup timestamp for offsets; only relevant when startup mode
     *     is {@link StartupMode#TIMESTAMP}.
     */
    protected KafkaTableSourceBase(
            TableSchema schema,
            Optional<String> proctimeAttribute,
            List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors,
            Optional<Map<String, String>> fieldMapping,
            String topic,
            Properties properties,
            DeserializationSchema<Row> deserializationSchema,
            StartupMode startupMode,
            Map<KafkaTopicPartition, Long> specificStartupOffsets,
            long startupTimestampMillis) {
        this.schema = TableSchemaUtils.checkNoGeneratedColumns(schema);
        this.proctimeAttribute = validateProctimeAttribute(proctimeAttribute);
        this.rowtimeAttributeDescriptors =
                validateRowtimeAttributeDescriptors(rowtimeAttributeDescriptors);
        this.fieldMapping = fieldMapping;
        this.topic = Preconditions.checkNotNull(topic, "Topic must not be null.");
        this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
        this.deserializationSchema =
                Preconditions.checkNotNull(
                        deserializationSchema, "Deserialization schema must not be null.");
        this.startupMode =
                Preconditions.checkNotNull(startupMode, "Startup mode must not be null.");
        this.specificStartupOffsets =
                Preconditions.checkNotNull(
                        specificStartupOffsets, "Specific offsets must not be null.");
        this.startupTimestampMillis = startupTimestampMillis;
    }

    /**
     * Creates a generic Kafka {@link StreamTableSource}.
     *
     * @param schema Schema of the produced table.
     * @param topic Kafka topic to consume.
     * @param properties Properties for the Kafka consumer.
     * @param deserializationSchema Deserialization schema for decoding records from Kafka.
     */
    protected KafkaTableSourceBase(
            TableSchema schema,
            String topic,
            Properties properties,
            DeserializationSchema<Row> deserializationSchema) {
        this(
                schema,
                Optional.empty(),
                Collections.emptyList(),
                Optional.empty(),
                topic,
                properties,
                deserializationSchema,
                StartupMode.GROUP_OFFSETS,
                Collections.emptyMap(),
                DEFAULT_STARTUP_TIMESTAMP_MILLIS);
    }

    /**
     * NOTE: This method is for internal use only for defining a TableSource. Do not use it in Table
     * API programs.
     */
    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {

        DeserializationSchema<Row> deserializationSchema = getDeserializationSchema();
        // Version-specific Kafka consumer
        FlinkKafkaConsumerBase<Row> kafkaConsumer =
                getKafkaConsumer(topic, properties, deserializationSchema);
        DataStream<Row> ds = env.addSource(kafkaConsumer).name(explainSource());

        int parallelism = Integer.parseInt(properties.getProperty(KafkaValidator.PARALLELISM));
        setParallelism(ds, parallelism);

        boolean needDelay = Boolean.parseBoolean(properties.getProperty(KafkaValidator.DELAY_OPEN));
        int keyIndex = Integer.parseInt(properties.getProperty(KafkaValidator.KEYBY_INDEX));
        String keyField = properties.getProperty(KafkaValidator.KEYBY_FIELD);
        KeySelector selector = getSelector(keyField, keyIndex);

        KeyedStream keyedStream = null;

        if (needDelay) {
            long delaySecond = Long.parseLong(properties.getProperty(KafkaValidator.DELAY_SECOND));
            int eventTimeFieldIndex =
                    Integer.parseInt(
                            properties.getProperty(KafkaValidator.DELAY_EVENTTIME_FIELD_INDEX));
            int delayCacheSize =
                    Integer.parseInt(properties.getProperty(KafkaValidator.DELAY_CACHE_SIZE));
            if (selector != null) {
                keyedStream =
                        ds.flatMap(
                                        new DelayFlatMapFunction(
                                                delaySecond, eventTimeFieldIndex, delayCacheSize))
                                .uid("delay-flatmap")
                                .keyBy(selector);
            } else {
                ds.flatMap(
                                new DelayFlatMapFunction(
                                        delaySecond, eventTimeFieldIndex, delayCacheSize))
                        .uid("delay-flatmap");
            }
            setParallelism(ds, parallelism);
            List<Transformation<?>> transformations;
            try {
                Field field = StreamExecutionEnvironment.class.getDeclaredField("transformations");
                field.setAccessible(true);
                transformations = (List<Transformation<?>>) field.get(env);

                Field transformation = DataStream.class.getDeclaredField("transformation");
                transformation.setAccessible(true);
                transformation.set(ds, transformations.get(transformations.size() - 1));
            } catch (Exception e) {
                throw new RuntimeException("Extract env transformation to datastream error.", e);
            }
        } else {
            if (selector != null) {
                keyedStream = ds.keyBy(selector);
            }
        }

        return keyedStream == null ? ds : keyedStream;
    }

    private KeySelector getSelector(String keyField, int keyIndex) {
        if (!StringUtils.isNullOrWhitespaceOnly(keyField) && keyIndex >= 0) {
            return new KeySelector<Row, Object>() {
                private static final long serialVersionUID = 7579778906167956025L;

                @Override
                public Object getKey(Row value) throws Exception {
                    Map<String, String> field = (Map<String, String>) value.getField(keyIndex);
                    return field.get(keyField);
                }
            };
        } else if (keyIndex >= 0) {
            return new KeySelector<Row, Object>() {
                private static final long serialVersionUID = -6935925117858124256L;

                @Override
                public Object getKey(Row value) throws Exception {
                    return value.getField(keyIndex);
                }
            };
        } else {
            return null;
        }
    }

    private void setParallelism(DataStream<Row> ds, int parallelism) {
        if (parallelism != 0) {
            ((SingleOutputStreamOperator<Row>) ds).setParallelism(parallelism);
        }
    }

    /** delay算子 */
    private static class DelayFlatMapFunction extends RichFlatMapFunction<Row, Row>
            implements CheckpointedFunction {

        private long delaySecond;

        private int eventTimeFieldIndex;

        private int delayCacheSize;

        private transient ListState<CacheRow> state;

        private transient PriorityBlockingQueue<CacheRow> cache;

        private transient ReentrantReadWriteLock readWriteLock;
        private transient Lock writeLock;
        private transient Lock readLock;

        @Override
        public void open(Configuration parameters) throws Exception {
            readWriteLock = new ReentrantReadWriteLock();
            writeLock = readWriteLock.writeLock();
            readLock = readWriteLock.readLock();
            super.open(parameters);
        }

        public DelayFlatMapFunction(long delaySecond, int eventTimeFieldIndex, int delayCacheSize) {
            this.delaySecond = delaySecond;
            this.eventTimeFieldIndex = eventTimeFieldIndex;
            this.delayCacheSize = delayCacheSize;
        }

        @Override
        public void flatMap(Row value, Collector<Row> out) throws Exception {
            try {
                readLock.lock();
                // 先处理一下缓存队列中的数据
                sendCacheRow(out, false);
                long eventTime = (Long) value.getField(eventTimeFieldIndex);
                // 可以发往下个算子的时间点
                long canSendTime = eventTime + delaySecond * 1000;
                if (needDelay(canSendTime)) {
                    // 队列已经满了
                    if (cache.size() >= delayCacheSize) {
                        sendCacheRow(out, true);
                    }
                    cache.offer(new CacheRow(canSendTime, value));
                } else {
                    out.collect(value);
                }
            } finally {
                readLock.unlock();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.clear();
            try {
                writeLock.lock();
                for (CacheRow cacheRow : cache) {
                    state.add(cacheRow);
                }
            } finally {
                writeLock.unlock();
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            initCache();
            ListStateDescriptor<CacheRow> descriptor =
                    new ListStateDescriptor<>(
                            "delay-message-cache", TypeInformation.of(CacheRow.class));
            state = context.getOperatorStateStore().getListState(descriptor);
            if (context.isRestored()) {
                for (CacheRow cacheRow : state.get()) {
                    cache.offer(cacheRow);
                }
            }
        }

        /**
         * 该数据是否需要延迟
         *
         * @param canSendTime
         * @return
         */
        public boolean needDelay(long canSendTime) {
            return canSendTime > System.currentTimeMillis();
        }

        /** 发送缓存中的数据 */
        public void sendCacheRow(Collector<Row> collector, boolean wait) {
            boolean hasSend = false;
            while (true) {
                CacheRow cacheRow = cache.peek();
                if (null == cacheRow) {
                    break;
                }

                long sleepMs = cacheRow.waitMs();
                if (sleepMs > 0) {
                    if (wait && !hasSend) {
                        try {
                            TimeUnit.MILLISECONDS.sleep(sleepMs);
                        } catch (InterruptedException e) {
                        }
                    } else {
                        break;
                    }
                }

                collector.collect(cacheRow.getRow());
                cache.poll();
                hasSend = true;
            }
        }

        private void initCache() {
            cache =
                    new PriorityBlockingQueue<>(
                            100000,
                            (o1, o2) -> {
                                if (o1.canSendTime == o2.canSendTime) {
                                    return 0;
                                } else if (o1.canSendTime > o2.canSendTime) {
                                    return 1;
                                } else {
                                    return -1;
                                }
                            });
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }

    /** 缓存对象 */
    public static class CacheRow {

        private long canSendTime;

        private Row row;

        public CacheRow() {}

        public CacheRow(long canSendTime, Row row) {
            this.canSendTime = canSendTime;
            this.row = row;
        }

        /** @return */
        public long waitMs() {
            return canSendTime - System.currentTimeMillis();
        }

        public long getCanSendTime() {
            return canSendTime;
        }

        public void setCanSendTime(long canSendTime) {
            this.canSendTime = canSendTime;
        }

        public Row getRow() {
            return row;
        }

        public void setRow(Row row) {
            this.row = row;
        }
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return deserializationSchema.getProducedType();
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public String getProctimeAttribute() {
        return proctimeAttribute.orElse(null);
    }

    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        return rowtimeAttributeDescriptors;
    }

    @Override
    public Map<String, String> getFieldMapping() {
        return fieldMapping.orElse(null);
    }

    @Override
    public String explainSource() {
        return TableConnectorUtils.generateRuntimeName(this.getClass(), schema.getFieldNames());
    }

    /**
     * Returns the properties for the Kafka consumer.
     *
     * @return properties for the Kafka consumer.
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Returns the deserialization schema.
     *
     * @return The deserialization schema
     */
    public DeserializationSchema<Row> getDeserializationSchema() {
        return deserializationSchema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final KafkaTableSourceBase that = (KafkaTableSourceBase) o;
        return Objects.equals(schema, that.schema)
                && Objects.equals(proctimeAttribute, that.proctimeAttribute)
                && Objects.equals(rowtimeAttributeDescriptors, that.rowtimeAttributeDescriptors)
                && Objects.equals(fieldMapping, that.fieldMapping)
                && Objects.equals(topic, that.topic)
                && Objects.equals(properties, that.properties)
                && Objects.equals(deserializationSchema, that.deserializationSchema)
                && startupMode == that.startupMode
                && Objects.equals(specificStartupOffsets, that.specificStartupOffsets)
                && startupTimestampMillis == that.startupTimestampMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schema,
                proctimeAttribute,
                rowtimeAttributeDescriptors,
                fieldMapping,
                topic,
                properties,
                deserializationSchema,
                startupMode,
                specificStartupOffsets,
                startupTimestampMillis);
    }

    /**
     * Returns a version-specific Kafka consumer with the start position configured.
     *
     * @param topic Kafka topic to consume.
     * @param properties Properties for the Kafka consumer.
     * @param deserializationSchema Deserialization schema to use for Kafka records.
     * @return The version-specific Kafka consumer
     */
    protected FlinkKafkaConsumerBase<Row> getKafkaConsumer(
            String topic, Properties properties, DeserializationSchema<Row> deserializationSchema) {
        FlinkKafkaConsumerBase<Row> kafkaConsumer =
                createKafkaConsumer(topic, properties, deserializationSchema);
        switch (startupMode) {
            case EARLIEST:
                kafkaConsumer.setStartFromEarliest();
                break;
            case LATEST:
                kafkaConsumer.setStartFromLatest();
                break;
            case GROUP_OFFSETS:
                kafkaConsumer.setStartFromGroupOffsets();
                break;
            case SPECIFIC_OFFSETS:
                kafkaConsumer.setStartFromSpecificOffsets(specificStartupOffsets);
                break;
            case TIMESTAMP:
                kafkaConsumer.setStartFromTimestamp(startupTimestampMillis);
                break;
        }
        kafkaConsumer.setCommitOffsetsOnCheckpoints(properties.getProperty("group.id") != null);
        return kafkaConsumer;
    }

    //////// VALIDATION FOR PARAMETERS

    /**
     * Validates a field of the schema to be the processing time attribute.
     *
     * @param proctimeAttribute The name of the field that becomes the processing time field.
     */
    private Optional<String> validateProctimeAttribute(Optional<String> proctimeAttribute) {
        return proctimeAttribute.map(
                (attribute) -> {
                    // validate that field exists and is of correct type
                    Optional<DataType> tpe = schema.getFieldDataType(attribute);
                    if (!tpe.isPresent()) {
                        throw new ValidationException(
                                "Processing time attribute '"
                                        + attribute
                                        + "' is not present in TableSchema.");
                    } else if (tpe.get().getLogicalType().getTypeRoot()
                            != LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
                        throw new ValidationException(
                                "Processing time attribute '"
                                        + attribute
                                        + "' is not of type TIMESTAMP.");
                    }
                    return attribute;
                });
    }

    /**
     * Validates a list of fields to be rowtime attributes.
     *
     * @param rowtimeAttributeDescriptors The descriptors of the rowtime attributes.
     */
    private List<RowtimeAttributeDescriptor> validateRowtimeAttributeDescriptors(
            List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors) {
        Preconditions.checkNotNull(
                rowtimeAttributeDescriptors, "List of rowtime attributes must not be null.");
        // validate that all declared fields exist and are of correct type
        for (RowtimeAttributeDescriptor desc : rowtimeAttributeDescriptors) {
            String rowtimeAttribute = desc.getAttributeName();
            Optional<DataType> tpe = schema.getFieldDataType(rowtimeAttribute);
            if (!tpe.isPresent()) {
                throw new ValidationException(
                        "Rowtime attribute '"
                                + rowtimeAttribute
                                + "' is not present in TableSchema.");
            } else if (tpe.get().getLogicalType().getTypeRoot()
                    != LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
                throw new ValidationException(
                        "Rowtime attribute '" + rowtimeAttribute + "' is not of type TIMESTAMP.");
            }
        }
        return rowtimeAttributeDescriptors;
    }

    //////// ABSTRACT METHODS FOR SUBCLASSES

    /**
     * Creates a version-specific Kafka consumer.
     *
     * @param topic Kafka topic to consume.
     * @param properties Properties for the Kafka consumer.
     * @param deserializationSchema Deserialization schema to use for Kafka records.
     * @return The version-specific Kafka consumer
     */
    protected abstract FlinkKafkaConsumerBase<Row> createKafkaConsumer(
            String topic, Properties properties, DeserializationSchema<Row> deserializationSchema);
}

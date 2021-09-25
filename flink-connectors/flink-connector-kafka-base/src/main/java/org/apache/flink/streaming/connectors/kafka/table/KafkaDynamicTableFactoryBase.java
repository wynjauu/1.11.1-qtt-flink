/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.*;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.*;

/**
 * Factory for creating configured instances of {@link KafkaDynamicSourceBase} and {@link
 * KafkaDynamicSinkBase}.
 */
public abstract class KafkaDynamicTableFactoryBase
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(
                        DeserializationFormatFactory.class, FactoryUtil.FORMAT);
        // Validate the option data type.
        helper.validateExcept(PROPERTIES_PREFIX);
        // Validate the option values.
        validateTableSourceOptions(tableOptions);

        DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

        final StartupOptions startupOptions = getStartupOptions(tableOptions);
        final Properties properties = getKafkaProperties(context.getCatalogTable().getOptions());
        // add topic-partition discovery
        properties.setProperty(
                FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,
                String.valueOf(
                        tableOptions
                                .getOptional(SCAN_TOPIC_PARTITION_DISCOVERY)
                                .map(Duration::toMillis)
                                .orElse(FlinkKafkaConsumerBase.PARTITION_DISCOVERY_DISABLED)));

        return createKafkaTableSource(
                producedDataType,
                KafkaOptions.getSourceTopics(tableOptions),
                KafkaOptions.getSourceTopicPattern(tableOptions),
                properties,
                decodingFormat,
                startupOptions.startupMode,
                startupOptions.specificOffsets,
                startupOptions.startupTimestampMillis);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();

        EncodingFormat<SerializationSchema<RowData>> encodingFormat =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT);

        // Validate the option values.
        validateTableSinkOptions(tableOptions);
        // Validate the option data type.
        helper.validateExcept(PROPERTIES_PREFIX);

        DataType consumedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
        return createKafkaTableSink(
                consumedDataType,
                tableOptions.get(TOPIC).get(0),
                getKafkaProperties(context.getCatalogTable().getOptions()),
                getFlinkKafkaPartitioner(tableOptions, context.getClassLoader()),
                encodingFormat,
                getSinkSemantic(tableOptions));
    }

    /**
     * Constructs the version-specific Kafka table source.
     *
     * @param producedDataType Source produced data type
     * @param topics Kafka topics to consume
     * @param topicPattern Kafka topic pattern to consume
     * @param properties Properties for the Kafka consumer
     * @param decodingFormat Decoding format for decoding records from Kafka
     * @param startupMode Startup mode for the contained consumer
     * @param specificStartupOffsets Specific startup offsets; only relevant when startup mode is
     *     {@link StartupMode#SPECIFIC_OFFSETS}
     */
    protected abstract KafkaDynamicSourceBase createKafkaTableSource(
            DataType producedDataType,
            @Nullable List<String> topics,
            @Nullable Pattern topicPattern,
            Properties properties,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            StartupMode startupMode,
            Map<KafkaTopicPartition, Long> specificStartupOffsets,
            long startupTimestampMillis);

    /**
     * Constructs the version-specific Kafka table sink.
     *
     * @param consumedDataType Sink consumed data type
     * @param topic Kafka topic to consume
     * @param properties Properties for the Kafka consumer
     * @param partitioner Partitioner to select Kafka partition for each item
     * @param encodingFormat Encoding format for encoding records to Kafka
     */
    protected abstract KafkaDynamicSinkBase createKafkaTableSink(
            DataType consumedDataType,
            String topic,
            Properties properties,
            Optional<FlinkKafkaPartitioner<RowData>> partitioner,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat,
            KafkaSinkSemantic semantic);

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FactoryUtil.FORMAT);
        options.add(PROPS_BOOTSTRAP_SERVERS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TOPIC);
        options.add(TOPIC_PATTERN);
        options.add(PROPS_GROUP_ID);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_SPECIFIC_OFFSETS);
        options.add(SCAN_TOPIC_PARTITION_DISCOVERY);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(SINK_PARTITIONER);
        options.add(DELAY_OPEN);
        options.add(DELAY_SECOND);
        options.add(DELAY_CACHE_SIZE);
        options.add(DELAY_EVENTTIME_FIELD_INDEX);
        options.add(PARALLELISM);
        options.add(SINK_SEMANTIC);
        return options;
    }
}

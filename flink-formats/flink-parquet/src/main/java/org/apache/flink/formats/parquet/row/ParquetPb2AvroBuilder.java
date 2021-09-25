package org.apache.flink.formats.parquet.row;

import net.qutoutiao.dataplatform.model.Field;
import net.qutoutiao.dataplatform.model.PB2Avro;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.utils.SerializableConfiguration;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.parquet.hadoop.ParquetOutputFormat.*;
import static org.apache.parquet.hadoop.codec.CodecConfig.getParquetCompressionCodec;

/**
 * @author hswang
 * @Date 2020-08-24 20:24
 */
public class ParquetPb2AvroBuilder extends ParquetWriter.Builder<RowData, ParquetPb2AvroBuilder> {

	private static final Logger LOG = LoggerFactory.getLogger(ParquetPb2AvroBuilder.class);
	private final RowType rowType;
	private final boolean utcTimestamp;
	private String[] fieldNames;


	public ParquetPb2AvroBuilder(
		OutputFile path,
		RowType rowType,
		boolean utcTimestamp) {
		super(path);
		this.rowType = rowType;
		this.utcTimestamp = utcTimestamp;

		fieldNames = rowType.getFieldNames().toArray(new String[0]);
		if (PB2Avro.SCHEMA$.getFields().size() != fieldNames.length) {
			throw new FlinkRuntimeException(String.format("You must provide a complete pb2 schema like s%,but your schema is s%", PB2Avro.SCHEMA$.toString(), rowType.toString()));
		}

	}

	@Override
	protected ParquetPb2AvroBuilder self() {
		return this;
	}

	@Override
	protected WriteSupport<RowData> getWriteSupport(Configuration conf) {
		return new QTTPb2AvroWriteSupport((new AvroSchemaConverter()).convert(PB2Avro.SCHEMA$), PB2Avro.SCHEMA$, GenericData.get());
	}

	private class QTTPb2AvroWriteSupport extends AvroWriteSupport<RowData> {

		private static final String KEY_VALUE = "value";
		private static final String KEY_JAVA_TYPE = "java_type";

		private static final String JAVA_TYPE_STRING = "STRING";
		private static final String JAVA_TYPE_LONG = "LONG";
		private static final String JAVA_TYPE_FLOAT = "FLOAT";
		private static final String JAVA_TYPE_INTEGER = "INTEGER";

		private static final String EMPTY_STRING = "";


		AvroWriteSupport<PB2Avro> writeSupport;

		public QTTPb2AvroWriteSupport(MessageType schema, Schema avroSchema, GenericData model) {
			super(schema, avroSchema, model);
			writeSupport = new AvroWriteSupport<>(schema, avroSchema, model);
		}

		@Override
		public WriteContext init(Configuration configuration) {
			writeSupport.init(configuration);
			return super.init(configuration);
		}

		@Override
		public void prepareForWrite(RecordConsumer recordConsumer) {
			writeSupport.prepareForWrite(recordConsumer);
			super.prepareForWrite(recordConsumer);
		}

		@Override
		public FinalizedWriteContext finalizeWrite() {
			writeSupport.finalizeWrite();
			return super.finalizeWrite();
		}

		@Override
		public void write(RowData record) {
			try {
				writeSupport.write(toPB2Avro(record));
			} catch (Exception e) {
				LOG.info("parse RowData to pb2 failed , record = {}", record.toString());
				throw e;
			}
		}

		private PB2Avro toPB2Avro(RowData record) {
			//TODO
			long logTimestamp = record.getLong(0);
			StringData ip = record.getString(1);
			MapData field = record.getMap(2);

			//field Map<STRING, MAP<STRING, STRING>>
			ArrayData keys = field.keyArray();
			ArrayData values = field.valueArray();

			//
			Map<CharSequence, Field> fields = new HashMap<>();

			int size = keys.size();
			for (int i = 0; i < size; i++) {
				Field fieldTmp = new Field();
				StringData key = keys.getString(i);

				//内层MAP就存储了两个KEY, value, java_type
				MapData map = values.getMap(i);

				ArrayData mapKey = map.keyArray();
				ArrayData mapValue = map.valueArray();

				String value = EMPTY_STRING; //key的值
				String javaType = EMPTY_STRING; //key的类型

				String valueMapKey = mapKey.getString(0).toString();
				if (KEY_VALUE.equals(valueMapKey)) {
					value = mapValue.getString(0).toString();
				} else {
					javaType = mapValue.getString(0).toString();
				}

				valueMapKey = mapKey.getString(1).toString();
				if (KEY_JAVA_TYPE.equals(valueMapKey)) {
					javaType = mapValue.getString(1).toString();
				} else {
					value = mapValue.getString(1).toString();
				}

				switch (javaType) {
					case JAVA_TYPE_STRING:
						fieldTmp.setStringType(value);
						break;
					case JAVA_TYPE_LONG:
						fieldTmp.setLongType(Long.valueOf(value));
						break;
					case JAVA_TYPE_FLOAT:
						fieldTmp.setFloatType(Float.valueOf(value));
						break;
					case JAVA_TYPE_INTEGER:
						fieldTmp.setIntType(Integer.valueOf(value));
						break;
					default:
						throw new RuntimeException("unknow java type [" + javaType + "]");
				}

				fields.put(key.toString(), fieldTmp);
			}

			PB2Avro pb2Avro = PB2Avro.newBuilder()
				.setLogTimestamp(logTimestamp)
				.setIp(ip.toString())
				.setField(fields)
				.build();
			return pb2Avro;
		}
	}

	public static ParquetWriterFactory<RowData> createWriterFactory(
		RowType rowType,
		Configuration conf,
		boolean utcTimestamp, boolean isCompact) {
		return new ParquetWriterFactory<>(
			new FlinkParquetPb2AvroBuilder(rowType, conf, utcTimestamp, isCompact));
	}

	public static class FlinkParquetPb2AvroBuilder implements ParquetBuilder<RowData> {

		private final RowType rowType;
		private final SerializableConfiguration configuration;
		private final boolean utcTimestamp;
		private final boolean isCompact;

		public FlinkParquetPb2AvroBuilder(
			RowType rowType,
			Configuration conf,
			boolean utcTimestamp, boolean isCompact) {
			this.rowType = rowType;
			this.configuration = new SerializableConfiguration(conf);
			this.utcTimestamp = utcTimestamp;
			this.isCompact = isCompact;
			if (isCompact) {
				throw new FlinkRuntimeException("protobuf 'message-v1-etl' does not support file compaction,please use 'message-v1-datax'.");
			}
		}

		@Override
		public ParquetWriter<RowData> createWriter(OutputFile out) throws IOException {
			Configuration conf = configuration.conf();
			return new ParquetPb2AvroBuilder(out, rowType, utcTimestamp)
				.withCompressionCodec(getParquetCompressionCodec(conf))
				.withRowGroupSize(getBlockSize(conf))
				.withPageSize(getPageSize(conf))
				.withDictionaryPageSize(getDictionaryPageSize(conf))
				.withMaxPaddingSize(conf.getInt(
					MAX_PADDING_BYTES, ParquetWriter.MAX_PADDING_SIZE_DEFAULT))
				.withDictionaryEncoding(getEnableDictionary(conf))
				.withValidation(getValidation(conf))
				.withWriterVersion(getWriterVersion(conf))
				.withConf(conf).build();
		}
	}
}

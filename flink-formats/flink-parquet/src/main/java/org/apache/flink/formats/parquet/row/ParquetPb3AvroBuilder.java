package org.apache.flink.formats.parquet.row;

import net.qutoutiao.dataplatform.model.PB3Avro;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.utils.SerializableConfiguration;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.HashMap;

import static org.apache.parquet.hadoop.ParquetOutputFormat.*;
import static org.apache.parquet.hadoop.codec.CodecConfig.getParquetCompressionCodec;

/**
 * @author hswang
 * @Date 2020-08-24 20:24
 */
public class ParquetPb3AvroBuilder extends ParquetWriter.Builder<RowData, ParquetPb3AvroBuilder> {
	private final RowType rowType;
	private final boolean utcTimestamp;
	private String[] fieldNames;


	public ParquetPb3AvroBuilder(
		OutputFile path,
		RowType rowType,
		boolean utcTimestamp) {
		super(path);
		this.rowType = rowType;
		this.utcTimestamp = utcTimestamp;

		fieldNames = rowType.getFieldNames().toArray(new String[0]);
//		if (PB3Avro.SCHEMA$.getFields().size() != fieldNames.length) {
//			throw new FlinkRuntimeException(String.format("You must provide a complete pb3 schema like %s,but your schema is %s", PB3Avro.SCHEMA$.toString(), rowType.toString()));
//		}
	}

	@Override
	protected ParquetPb3AvroBuilder self() {
		return this;
	}

	@Override
	protected WriteSupport<RowData> getWriteSupport(Configuration conf) {
		return new QTTPb3AvroWriteSupport((new AvroSchemaConverter()).convert(PB3Avro.SCHEMA$), PB3Avro.SCHEMA$, GenericData.get());
	}

	private class QTTPb3AvroWriteSupport extends AvroWriteSupport<RowData> {


		AvroWriteSupport<GenericRecord> writeSupport;

		private Schema avroSchema;

		public QTTPb3AvroWriteSupport(MessageType schema, Schema avroSchema, GenericData model) {
			super(schema, avroSchema, model);
			this.avroSchema = avroSchema;
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
			writeSupport.write(toGenericRecord(record));
		}

		private GenericRecord toGenericRecord(RowData record) {
			GenericRecord genericRecord = new GenericData.Record(avroSchema);
			GenericRowData rowData = (GenericRowData) record;
			int columnCnt = fieldNames.length;
			for (int i = 0; i < columnCnt; i++) {
				genericRecord.put(fieldNames[i], getValueWithType(rowData, i, rowType.getTypeAt(i)));
			}
			return genericRecord;
		}


		public Object getValueWithType(GenericRowData record, int index, LogicalType type) {
			Object object = record.getField(index);
			if (object == null) {
				return null;
			}
			switch (type.getTypeRoot()) {
				case BIGINT:
					return record.getLong(index);
				case MAP:
					return toHashMap(record.getMap(index));
				default:
					return record.getString(index).toString();
			}
		}

		/**
		 * MapData -> HashMap
		 *
		 * @param mapData
		 * @return
		 */
		HashMap<String, String> toHashMap(MapData mapData) {
			HashMap<String, String> map = new HashMap<>();
			ArrayData keys = mapData.keyArray();
			ArrayData values = mapData.valueArray();

			for (int i = 0; i < keys.size(); i++) {
				map.put(keys.getString(i).toString(), values.getString(i).toString());
			}

			return map;
		}
	}

	public static ParquetWriterFactory<RowData> createWriterFactory(
		RowType rowType,
		Configuration conf,
		boolean utcTimestamp) {
		return new ParquetWriterFactory<>(
			new FlinkParquetPb3AvroBuilder(rowType, conf, utcTimestamp));
	}

	public static class FlinkParquetPb3AvroBuilder implements ParquetBuilder<RowData> {

		private final RowType rowType;
		private final SerializableConfiguration configuration;
		private final boolean utcTimestamp;

		public FlinkParquetPb3AvroBuilder(
			RowType rowType,
			Configuration conf,
			boolean utcTimestamp) {
			this.rowType = rowType;
			this.configuration = new SerializableConfiguration(conf);
			this.utcTimestamp = utcTimestamp;
		}

		@Override
		public ParquetWriter<RowData> createWriter(OutputFile out) throws IOException {
			Configuration conf = configuration.conf();
			return new ParquetPb3AvroBuilder(out, rowType, utcTimestamp)
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

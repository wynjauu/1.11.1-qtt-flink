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
 * @author: zhushang
 * @create: 2021-01-04 16:24
 **/

public class ParquetPb2DataXBuilder extends ParquetWriter.Builder<RowData, ParquetPb2DataXBuilder> {
	private static final Logger LOG = LoggerFactory.getLogger(ParquetPb2DataXBuilder.class);
	private final RowType rowType;
	private String[] fieldNames;

	protected ParquetPb2DataXBuilder(OutputFile path, RowType rowType) {
		super(path);
		this.rowType = rowType;
		fieldNames = rowType.getFieldNames().toArray(new String[0]);
		if (PB2Avro.SCHEMA$.getFields().size() != fieldNames.length) {
			throw new FlinkRuntimeException(String.format("You must provide a complete pb2 schema like s%,but your schema is s%", PB2Avro.SCHEMA$.toString(), rowType.toString()));
		}
	}


	/**
	 * @return this as the correct subclass of ParquetWriter.Builder.
	 */
	@Override
	protected ParquetPb2DataXBuilder self() {
		return this;
	}

	/**
	 * @param conf a configuration
	 * @return an appropriate WriteSupport for the object model.
	 */
	@Override
	protected WriteSupport<RowData> getWriteSupport(Configuration conf) {
		return new ParquetPb2DataXBuilder.QTTPb2AvroWriteSupport((new AvroSchemaConverter()).convert(PB2Avro.SCHEMA$), PB2Avro.SCHEMA$, GenericData.get());
	}


	private class QTTPb2AvroWriteSupport extends AvroWriteSupport<RowData> {
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
			MapData field = record.getMap(2);

			ArrayData keys = field.keyArray();
			ArrayData values = field.valueArray();

			Map<CharSequence, Field> fields = new HashMap<>();

			int size = keys.size();
			for (int i = 0; i < size; i++) {
				Field fieldTmp = new Field();
				StringData key = keys.getString(i);
				RowData rowData = values.getRow(i, 4);
				fieldTmp.setIntType(rowData.isNullAt(0) ? null : rowData.getInt(0));
				fieldTmp.setLongType(rowData.isNullAt(1) ? null : rowData.getLong(1));
				fieldTmp.setFloatType(rowData.isNullAt(2) ? null : rowData.getFloat(2));
				fieldTmp.setStringType(rowData.isNullAt(3) ? null : rowData.getString(3).toString());
				fields.put(key.toString(), fieldTmp);
			}

			PB2Avro pb2Avro = PB2Avro.newBuilder()
				.setLogTimestamp(record.getLong(0))
				.setIp(record.getString(1).toString())
				.setField(fields)
				.build();
			return pb2Avro;

		}


	}

	public static ParquetWriterFactory<RowData> createWriterFactory(
		RowType rowType,
		Configuration conf) {
		return new ParquetWriterFactory<>(
			new ParquetPb2DataXBuilder.FlinkParquetPb2AvroBuilder(rowType, conf));
	}

	public static class FlinkParquetPb2AvroBuilder implements ParquetBuilder<RowData> {

		private final RowType rowType;
		private final SerializableConfiguration configuration;

		public FlinkParquetPb2AvroBuilder(
			RowType rowType,
			Configuration conf) {
			this.rowType = rowType;
			this.configuration = new SerializableConfiguration(conf);
		}

		@Override
		public ParquetWriter<RowData> createWriter(OutputFile out) throws IOException {
			Configuration conf = configuration.conf();
			return new ParquetPb2DataXBuilder(out, rowType)
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

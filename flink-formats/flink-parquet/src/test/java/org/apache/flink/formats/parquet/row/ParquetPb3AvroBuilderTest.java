package org.apache.flink.formats.parquet.row;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.vector.ParquetColumnarRowSplitReader;
import org.apache.flink.formats.parquet.vector.ParquetSplitReaderUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

/**
 * @author: zhushang
 * @create: 2020-12-28 17:39
 **/

public class ParquetPb3AvroBuilderTest {
	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private static final RowType ROW_TYPE = RowType.of(
		new LogicalType[]{
			new VarCharType(VarCharType.MAX_LENGTH),
			new VarCharType(VarCharType.MAX_LENGTH),
			new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH)}, new String[]{"log_id", "session_id", "event_time", "app"});

	@SuppressWarnings("unchecked")
	private static final DataFormatConverters.DataFormatConverter<RowData, Row> CONVERTER =
		DataFormatConverters.getConverterForDataType(
			TypeConversions.fromLogicalToDataType(ROW_TYPE));

	@Test
	public void testTypes() throws IOException {
		Configuration conf = new Configuration();
		innerTest(conf, true);
		innerTest(conf, false);
	}

	@Test
	public void testCompression() throws IOException {
		Configuration conf = new Configuration();
		conf.set(ParquetOutputFormat.COMPRESSION, "GZIP");
		innerTest(conf, true);
		innerTest(conf, false);
	}

	private void innerTest(
		Configuration conf,
		boolean utcTimestamp) throws IOException {
		Path path = new Path(TEMPORARY_FOLDER.newFolder().getPath(), UUID.randomUUID().toString());
		int number = 10;
		List<Row> rows = new ArrayList<>(number);
		for (int i = 0; i < number; i++) {
			Integer v = i;
			rows.add(Row.of(
				String.valueOf(v),
				v + "+2",
				v.longValue(), null));
		}

		ParquetWriterFactory<RowData> factory = ParquetPb3AvroBuilder.createWriterFactory(
			ROW_TYPE, conf, utcTimestamp);
		BulkWriter<RowData> writer = factory.create(path.getFileSystem().create(
			path, FileSystem.WriteMode.OVERWRITE));
		for (int i = 0; i < number; i++) {
			writer.addElement(CONVERTER.toInternal(rows.get(i)));
		}
		writer.flush();
		writer.finish();

		// verify
		ParquetColumnarRowSplitReader reader = ParquetSplitReaderUtil.genPartColumnarRowReader(
			utcTimestamp,
			true,
			conf,
			ROW_TYPE.getFieldNames().toArray(new String[0]),
			ROW_TYPE.getChildren().stream()
				.map(TypeConversions::fromLogicalToDataType)
				.toArray(DataType[]::new),
			new HashMap<>(),
			IntStream.range(0, ROW_TYPE.getFieldCount()).toArray(),
			50,
			path,
			0,
			Long.MAX_VALUE);
		int cnt = 0;
		while (!reader.reachedEnd()) {
			Row row = CONVERTER.toExternal(reader.nextRecord());
			Assert.assertEquals(rows.get(cnt), row);
			cnt++;
		}
		Assert.assertEquals(number, cnt);
	}

	private LocalDateTime toDateTime(Integer v) {
		v = (v > 0 ? v : -v) % 1000;
		return LocalDateTime.now().plusNanos(v).plusSeconds(v);
	}
}

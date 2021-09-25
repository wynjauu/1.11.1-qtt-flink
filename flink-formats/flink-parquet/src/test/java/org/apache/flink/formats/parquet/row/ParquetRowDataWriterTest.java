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

package org.apache.flink.formats.parquet.row;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetRowDataInputFormat;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.utils.ParquetSchemaConverter;
import org.apache.flink.formats.parquet.vector.ParquetColumnarRowSplitReader;
import org.apache.flink.formats.parquet.vector.ParquetSplitReaderUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.IntStream;

/** Test for {@link ParquetRowDataBuilder} and {@link ParquetRowDataWriter}. */
public class ParquetRowDataWriterTest {

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private static final RowType ROW_TYPE =
            RowType.of(
                    new VarCharType(VarCharType.MAX_LENGTH),
                    new VarBinaryType(VarBinaryType.MAX_LENGTH),
                    new BooleanType(),
                    new TinyIntType(),
                    new SmallIntType(),
                    new IntType(),
                    new BigIntType(),
                    new FloatType(),
                    new DoubleType(),
                    new TimestampType(9),
                    new DecimalType(5, 0),
                    new DecimalType(15, 0),
                    new DecimalType(20, 0));

    private static final List<RowType.RowField> fields =
            Arrays.asList(
                    new RowType.RowField("int_type", new IntType()),
                    new RowType.RowField("long_type", new BigIntType()),
                    new RowType.RowField("float_type", new FloatType()),
                    new RowType.RowField("string_type", new VarCharType(VarCharType.MAX_LENGTH)));

    private static final List<RowType.RowField> fields1 =
            Arrays.asList(
                    new RowType.RowField("r1", new IntType()),
                    new RowType.RowField("r2", new BigIntType()));
    private static final RowType TYPE =
            RowType.of(
                    new RowType(fields),
                    new BigIntType(),
                    new VarCharType(VarCharType.MAX_LENGTH),
                    //                    new ArrayType(new VarCharType(VarCharType.MAX_LENGTH)),
                    new ArrayType(new RowType(fields1)),
                    new MapType(new VarCharType(VarCharType.MAX_LENGTH), new RowType(fields)));

    @SuppressWarnings("unchecked")
    private static final DataFormatConverters.DataFormatConverter<RowData, Row> CONVERTER =
            DataFormatConverters.getConverterForDataType(
                    TypeConversions.fromLogicalToDataType(ROW_TYPE));

    private static final DataFormatConverters.DataFormatConverter<RowData, Row> CONVERTER1 =
            DataFormatConverters.getConverterForDataType(
                    TypeConversions.fromLogicalToDataType(TYPE));

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

    @Test
    public void testPb2() throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(TEMPORARY_FOLDER.newFolder().getPath(), UUID.randomUUID().toString());
        Map<String, Row> map = new HashMap<>();
        map.put("key", Row.of(null, null, null, "string"));
        Row row =
                Row.of(
                        Row.of(1, null, 2.0F, "tttt"),
                        1312312414324L,
                        "ip",
                        new Row[] {Row.of(1, 2L), Row.of(3, 4L)},
                        //                        new String[] {"s1", "s2"},
                        map);
        System.out.println("input is :" + row);
        ParquetWriterFactory<RowData> factory =
                ParquetRowDataBuilder.createWriterFactory(TYPE, conf, true);
        BulkWriter<RowData> writer =
                factory.create(path.getFileSystem().create(path, FileSystem.WriteMode.OVERWRITE));
        writer.addElement(CONVERTER1.toInternal(row));
        writer.flush();
        writer.finish();
        MessageType messageType =
                ParquetSchemaConverter.toParquetType(
                        TypeConversions.fromLogicalToDataType(
                                TYPE.getChildren().toArray(new LogicalType[0])),
                        false,
                        TYPE.getFieldNames().toArray(new String[0]));
        System.out.println("message type :" + messageType.toString());
        InnerInputFormat inputFormat = new InnerInputFormat(path, messageType);
        while (!inputFormat.reachedEnd()) {
            Row row1 = CONVERTER1.toExternal(inputFormat.nextRecord(null));
            System.out.println("output is :" + row1.toString());
        }
    }

    private void innerTest(Configuration conf, boolean utcTimestamp) throws IOException {
        Path path = new Path(TEMPORARY_FOLDER.newFolder().getPath(), UUID.randomUUID().toString());
        int number = 1000;
        List<Row> rows = new ArrayList<>(number);
        for (int i = 0; i < number; i++) {
            Integer v = i;
            rows.add(
                    Row.of(
                            String.valueOf(v),
                            String.valueOf(v).getBytes(StandardCharsets.UTF_8),
                            v % 2 == 0,
                            v.byteValue(),
                            v.shortValue(),
                            v,
                            v.longValue(),
                            v.floatValue(),
                            v.doubleValue(),
                            toDateTime(v),
                            BigDecimal.valueOf(v),
                            BigDecimal.valueOf(v),
                            BigDecimal.valueOf(v)));
        }

        ParquetWriterFactory<RowData> factory =
                ParquetRowDataBuilder.createWriterFactory(ROW_TYPE, conf, utcTimestamp);
        BulkWriter<RowData> writer =
                factory.create(path.getFileSystem().create(path, FileSystem.WriteMode.OVERWRITE));
        for (int i = 0; i < number; i++) {
            writer.addElement(CONVERTER.toInternal(rows.get(i)));
        }
        writer.flush();
        writer.finish();

        // verify
        ParquetColumnarRowSplitReader reader =
                ParquetSplitReaderUtil.genPartColumnarRowReader(
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

    private class InnerInputFormat extends ParquetRowDataInputFormat {

        /**
         * Read parquet files with given parquet file schema.
         *
         * @param path The path of the file to read.
         * @param messageType schema of parquet file
         */
        protected InnerInputFormat(Path path, MessageType messageType) throws IOException {
            super(path, messageType);
            open(new FileInputSplit(1, path, 0, path.depth(), new String[] {""}));
        }
    }
}

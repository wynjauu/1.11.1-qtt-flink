package org.apache.flink.formats.protobuf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.junit.Test;

/**
 * Created by liufangliang on 2020/8/18.
 */
public class ProtoBufRowDataDeserializationSchemaTest {

	private static final TableSchema SCHEMA = TableSchema.builder()
		.field("log_timestamp", DataTypes.BIGINT())
		.field("ip", DataTypes.STRING())
		.field("field", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
		.build();

	private static final RowType ROW_TYPE = (RowType) SCHEMA.toRowDataType().getLogicalType();

	private static final TableSchema PB2 = TableSchema.builder()
		.field("protocol_version", DataTypes.STRING())
		.field("module", DataTypes.STRING())
		.field("event_time", DataTypes.BIGINT())
		.field("client_ip", DataTypes.STRING())
		.field("mid", DataTypes.STRING())
		.field("extend_info", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
		.field("arrive_time", DataTypes.BIGINT())
		.field("dtu", DataTypes.STRING())
		.field("topic", DataTypes.STRING())
		.build();

	private static final RowType ROW_TYPE2 = (RowType) PB2.toRowDataType().getLogicalType();

	@Test
	public void deserializePb1() throws Exception {

		RowTypeInfo rowTypeInfo = (RowTypeInfo) SCHEMA.toRowType();

		ScanRuntimeProviderContext context = new ScanRuntimeProviderContext();

		TypeInformation<Row> typeInformation = (TypeInformation<Row>) context.createTypeInformation(SCHEMA.toRowDataType());
		TypeInformation<RowData> typeInformation2 = (TypeInformation<RowData>) context.createTypeInformation(SCHEMA.toRowDataType());

		ProtoBufRowDataDeserializationSchema protoBufRowDataDeserializationSchema = new ProtoBufRowDataDeserializationSchema(
			ROW_TYPE,
			typeInformation2,
			"message-v1",
			false);

		byte[] v1 = new ProtoBufRowDeserializationSchemaTest().messageToBytes();

		RowData deserialize = protoBufRowDataDeserializationSchema.deserialize(v1);
		System.out.println(deserialize);

	}


	@Test
	public void deserializePb2() throws Exception {

		ScanRuntimeProviderContext context = new ScanRuntimeProviderContext();

		TypeInformation<Row> typeInformation = (TypeInformation<Row>) context.createTypeInformation(PB2.toRowDataType());
		TypeInformation<RowData> typeInformation2 = (TypeInformation<RowData>) context.createTypeInformation(PB2.toRowDataType());

		ProtoBufRowDataDeserializationSchema protoBufRowDataDeserializationSchema = new ProtoBufRowDataDeserializationSchema(
			ROW_TYPE2,
			typeInformation2,
			"message-v2",
			false);

		byte[] v2 = new ProtoBufRowDeserializationSchemaTest().newMessageV3ToBytes().toByteArray();

		RowData deserialize = protoBufRowDataDeserializationSchema.deserialize(v2);
		System.out.println(deserialize);

	}

}

package org.apache.flink.formats.protobuf;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.protobuf.ProtoBufRowDeserializationSchema;
import org.apache.flink.formats.protobuf.message.NewMessageV3;
import org.apache.flink.table.descriptors.Protobuf;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by liufangliang on 2020/6/1.
 */
public class ProtobufRowFormatFactoryTest {
	@Test
	public void createSerializationSchema() throws Exception {
	}

	@Test
	public void createDeserializationSchema() throws Exception {
		final String messageVersion = "message-v3";
		final Boolean ignoreParseErrors = false;
		final TypeInformation<Row> information = Types.ROW_NAMED(new String[]{"protocol_version", "log_id"}
			, new TypeInformation[]{Types.STRING, Types.STRING});

		final Protobuf protobuf = new Protobuf()
			.setIgnoreParseErrors(ignoreParseErrors)
			.setQttMessageVersion(messageVersion)
			.setTypeInformation(information);

		final DeserializationSchema<?> expectedSer = TableFactoryService
			.find(DeserializationSchemaFactory.class, protobuf.toProperties())
			.createDeserializationSchema(protobuf.toProperties());


		final ProtoBufRowDeserializationSchema actualSer = new ProtoBufRowDeserializationSchema
			.Builder()
			.setTypeInfo(information)
			.setIgnoreParseErrors(ignoreParseErrors)
			.setMessageVersion(messageVersion)
			.build();
		assertEquals(actualSer, expectedSer);


	}

	@Test
	public void concertMessageV3MessageToRow() throws Exception {

		// protobuf message
		NewMessageV3.Log.Builder builder = NewMessageV3.Log.newBuilder();
		Map<String, String> map = new HashMap<>(3);
		map.put("mapk1", "mapve2");
		map.put("mapk2", "13");
		map.put("mapk3", "12.5D");
		builder.putAllExtendInfo(map);

		builder.setProtocolVersion("2.0");
		builder.setLogId("9e0f6fc8e1db469483c80c3380cf1d04|116405");
		builder.setSessionId("fab950a8c5a64bd99b5ec230461ef6d9");
		builder.setEventTime(1576120315113L);
		builder.setClientIp("221.9.68.179");
		builder.setElement("video_play_duration");
		builder.setApp("DDAndroid");
		builder.setPage("/feed/home");
		builder.setModule("modele");
		builder.setAction("show");
		builder.setReferer("referer");
		builder.setOs("os");
		builder.setDevice("devices");
		builder.setDistinctId("distinctId");
		builder.setTk("ACFd0XUKmIxLlqySU85F6SCkcw64__dAohFkZHNw");
		builder.setMid("144945352");
		builder.setUuid("uid");
		builder.setAppVersion("1.08.2.1114.2141");
		builder.setOsVersion("7.1.1");
		builder.setDeviceMode("OPPO+R11+Plusk");
		builder.setDeviceManu("oppeo");
		builder.setOsCode(25);
		builder.setDeviceBrand("OPPO");
		builder.setScreenWidth("1080");
		builder.setScreenHeight("1920");
		builder.setDeviceCarrier("mobile");
		builder.setDtu("oppo");
		builder.setNetwork("wifi");
//		builder.setVestName("vestname");
//		builder.setEnv("test");
//		builder.setLat("44.212321");
//		builder.setLon("125.232212");
//		builder.setAppSubversion("appsubversion");
//		builder.setTopic("kafka_topic_name");
//		builder.setArriveTime(1576120326446L);
//		builder.setEvent("click event");
//		builder.setAndroidId("android Id");
//		builder.setTuid("a7882228512105ec");
//		builder.setPlatform("qtt");

		// protobuf schema
		final String messageVersion = "message-v2";
		final Boolean ignoreParseErrors = false;
		final TypeInformation<Row> information = Types.ROW_NAMED(new String[]{"protocol_version_error", "arrive_time", "event_time", "extend_info"}
			, new TypeInformation[]{Types.STRING, Types.LONG, Types.LONG, Types.MAP(Types.STRING, Types.STRING)});
		final Protobuf protobuf = new Protobuf()
			.setIgnoreParseErrors(ignoreParseErrors)
			.setQttMessageVersion(messageVersion)
			.setTypeInformation(information);

		final DeserializationSchema<?> expectedSer = TableFactoryService
			.find(DeserializationSchemaFactory.class, protobuf.toProperties())
			.createDeserializationSchema(protobuf.toProperties());


		Row row = (Row) expectedSer.deserialize(builder.build().toByteArray());


		Row row1 = new Row(4);
		row1.setField(0, "2.0");
		row1.setField(1, "9e0f6fc8e1db469483c80c3380cf1d04|116405");
		row1.setField(2, 1576120315113L);
		row1.setField(3, map);

		assertEquals(row,row1);


	}
}

package org.apache.flink.formats.protobuf;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.protobuf.ProtoBufRowDeserializationSchema;
import org.apache.flink.formats.protobuf.message.Message;
import org.apache.flink.formats.protobuf.message.NewMessageV3;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.Protobuf;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Created by liufangliang on 2020/6/5.
 */
public class ProtoBufRowDeserializationSchemaTest {

	@Test
	public void formatSchema() throws Exception {
		final TypeInformation<Row> information = Types.ROW_NAMED(new String[]{"protocol_version", "log_id", "event_time", "extend_info"}
			, new TypeInformation[]{Types.STRING, Types.STRING, Types.LONG, Types.MAP(Types.STRING, Types.STRING)});

		TableSchema schema = TableSchema.builder().field("protocol_version", DataTypes.STRING())
			.field("log_id", DataTypes.STRING())
			.field("event_time", DataTypes.BIGINT())
			.field("extend_info", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())).build();

		TypeInformation<?>[] fieldTypes = schema.getFieldTypes();
		String[] fieldNames = schema.getFieldNames();
		RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);

		assertEquals(rowTypeInfo, (RowTypeInfo) information);
	}

	@Test
	public void deserializeMessageV1() throws Exception {

		final String messageVersion = "message-v1";
		final Boolean ignoreParseErrors = false;
		final TypeInformation<Row> information = Types.ROW_NAMED(new String[]{"protocol_version", "log_id", "event_time", "extend_info"}
			, new TypeInformation[]{Types.STRING, Types.STRING, Types.LONG, Types.MAP(Types.STRING, Types.STRING)});

		final Protobuf protobuf = new Protobuf()
			.setIgnoreParseErrors(ignoreParseErrors)
			.setQttMessageVersion(messageVersion)
			.setTypeInformation(information)
			.setDeriveSchema(true);


		final DeserializationSchema<?> expectedSer = TableFactoryService
			.find(DeserializationSchemaFactory.class, protobuf.toProperties())
			.createDeserializationSchema(protobuf.toProperties());


		byte[] bytes = messageToBytes();

		Row deserialize = (Row) expectedSer.deserialize(bytes);

		System.out.println(deserialize);

	}

	@Test
	public void descrialize2() throws Exception {
		final String messageVersion = "message-v2";
		final Boolean ignoreParseErrors = false;
		final TypeInformation<Row> information = Types.ROW_NAMED(new String[]{"protocol_version", "log_id", "event_time", "extend_info"}
			, new TypeInformation[]{Types.STRING, Types.STRING, Types.LONG, Types.MAP(Types.STRING, Types.STRING)});


		final Protobuf protobuf = new Protobuf()
			.setIgnoreParseErrors(ignoreParseErrors)
			.setQttMessageVersion(messageVersion)
			.setTypeInformation(information)
			.setDeriveSchema(true);


		final DeserializationSchema<?> expectedSer = TableFactoryService
			.find(DeserializationSchemaFactory.class, protobuf.toProperties())
			.createDeserializationSchema(protobuf.toProperties());


		byte[] bytes = newMessageV3ToBytes().toByteArray();

		Object deserialize = expectedSer.deserialize(bytes);
		System.out.println(deserialize);

	}

	@Test
	public void createMessageV3ToKafka() {
		String topic = "flinksql";
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);
		try {
			while (true) {
				byte[] bytes = newMessageV3ToBytes().toByteArray();
				System.out.println(
					String.format("create message : %s ", bytes)
				);
				ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, bytes);
				producer.send(record);
				Thread.sleep(5000L);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}
	}

	@Test
	public void createMessageV1ToKafka() {
		String topic = "messagev1";
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);
		try {
			while (true) {
				byte[] bytes = messageToBytes();
//				System.out.println(
//					String.format("create message : %s ", bytes)
//				);
				ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, bytes);
				producer.send(record);
				Thread.sleep(3000L);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}
	}

	@Test
	public void readMessageFromKafka() {

		final String messageVersion = "message-v3";
		final Boolean ignoreParseErrors = false;
		final TypeInformation<Row> information = Types.ROW_NAMED(new String[]{"protocol_version", "arrive_time", "event_time", "extend_info"}
			, new TypeInformation[]{Types.BOOLEAN, Types.LONG, Types.LONG, Types.MAP(Types.STRING, Types.STRING)});

		ProtoBufRowDeserializationSchema build = new ProtoBufRowDeserializationSchema
			.Builder()
			.setMessageVersion(messageVersion)
			.setTypeInfo(information)
			.setIgnoreParseErrors(ignoreParseErrors)
			.build();


		String topic = "flinksql";
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(Collections.singletonList(topic));
		try {
			while (true) {
				ConsumerRecords<String, byte[]> records = consumer.poll(1000L);
				for (ConsumerRecord<String, byte[]> record : records) {
					System.out.println(
						String.format("topic : %s , offset : %d , message : %s ;"
							, record.topic()
							, record.offset()
							, build.deserialize(record.value()).toString()));
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}

	public byte[] messageToBytes() {
		Message.Log.Builder builder = Message.Log.newBuilder();
		builder.setIp("172.0.0.1");
		builder.setLogTimestamp(System.currentTimeMillis());

		Message.Log.Field.Builder field = Message.Log.Field.newBuilder();


		field.addMap(Message.Log.Field.Map.newBuilder()
			.setKey("name")
			.setValue(Message
				.ValueType
				.newBuilder()
				.setStringType(new Random().nextInt(5) + "明同学"))
			.build());

		field.addMap(Message.Log.Field.Map.newBuilder()
			.setKey("age")
			.setValue(Message
				.ValueType
				.newBuilder()
				.setIntType(new Random().nextInt(100)))
			.build());


		field.addMap(Message.Log.Field.Map.newBuilder()
			.setKey("height")
			.setValue(Message
				.ValueType
				.newBuilder()
				.setFloatType(new Random().nextFloat() * 100 + 100))
			.build());

		field.addMap(Message.Log.Field.Map.newBuilder()
			.setKey("timestamp")
			.setValue(Message
				.ValueType
				.newBuilder()
				.setLongType(System.currentTimeMillis()))
			.build());

		builder.setField(field);
		return builder.build().toByteArray();

	}

	public NewMessageV3.Log newMessageV3ToBytes() {
		NewMessageV3.Log.Builder builder = NewMessageV3.Log.newBuilder();
		Map<String, String> map = new HashMap<>(3);
		map.put("mapk1", "mapve2");
		map.put("mapk2", "13");
		map.put("mapk3", "12.5D");
		builder.putAllExtendInfo(map);

		builder.setProtocolVersion("2.0");
		builder.setLogId("9e0f6fc8e1db469483c80c3380cf1d04|116405");
		builder.setSessionId("fab950a8c5a64bd99b5ec230461ef6d9");
		builder.setEventTime(System.currentTimeMillis());
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
		builder.setArriveTime(1576120326446L);
//		builder.setEvent("click event");
//		builder.setAndroidId("android Id");
//		builder.setTuid("a7882228512105ec");
//		builder.setPlatform("qtt");

		return builder.build();
	}
}

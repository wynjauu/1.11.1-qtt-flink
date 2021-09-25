package org.apache.flink.formats.protobuf.messageutils;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.formats.protobuf.exception.ProtobufException;
import org.apache.flink.formats.protobuf.message.NewMessageV3;
import org.apache.flink.formats.protobuf.messageutils.ProtobufRowSchemaConverter;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by liufangliang on 2020/6/4.
 */
public class ProtobufRowSchemaConverterTest {

	@Test
	public void findClazzName() throws Exception {
	}

	@Test
	public void createDynamicMessage() throws Exception {

		final String messageVersion = "message-v3";
		ProtobufRowSchemaConverter build = new ProtobufRowSchemaConverter
			.Builder()
			.setMessageVersion(messageVersion)
			.build();

		byte[] bytes = newMessageV3ToBytes();
		DynamicMessage dynamicMessage = null;

		// message fields
		List<Descriptors.FieldDescriptor> fields = dynamicMessage.getDescriptorForType().getFields();

		for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : dynamicMessage.getAllFields().entrySet()) {
			Descriptors.FieldDescriptor key = entry.getKey();

			// to proto
			DescriptorProtos.FieldDescriptorProto fieldDescriptorProto = key.toProto();

//			Object defaultValue = key.getDefaultValue();
			//  e.g  net.qutoutiao.dataplatform.data.Log.protocol_version
			String fullName = key.getFullName();
			Descriptors.FieldDescriptor.JavaType javaType = key.getType().getJavaType();
			// e.g STRING
			String name = javaType.name();

			if(name.equals("MESSAGE")){
				// cannot be parsed when the type is message
			}
			// e.g value
			Object value = entry.getValue();

			System.out.println(String.format("key : %s , value : %s , javaType : %s", fullName, value, name));
		}
	}

	@Test
	public void createNewMessageV3(){
		byte[] bytes = newMessageV3ToBytes();

		try {
			NewMessageV3.Log log = NewMessageV3.Log.parseFrom(bytes);

			Map<Descriptors.FieldDescriptor, Object> allFields = log.getAllFields();
		} catch (InvalidProtocolBufferException e) {
			throw new ProtobufException("message-v3 parse exception",e);
		}



	}


	public byte[] newMessageV3ToBytes() {
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
//		builder.setArriveTime(1576120326446L);
//		builder.setEvent("click event");
//		builder.setAndroidId("android Id");
//		builder.setTuid("a7882228512105ec");
//		builder.setPlatform("qtt");

		return builder.build().toByteArray();
	}
}

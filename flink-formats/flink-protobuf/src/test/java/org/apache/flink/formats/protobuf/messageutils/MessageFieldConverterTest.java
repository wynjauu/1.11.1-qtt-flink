package org.apache.flink.formats.protobuf.messageutils;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.flink.formats.protobuf.ProtoBufRowDeserializationSchemaTest;
import org.apache.flink.formats.protobuf.message.Message;
import org.junit.Test;

import java.util.Map;

/**
 * Created by liufangliang on 2020/6/15.
 */
public class MessageFieldConverterTest {
	@Test
	public void convertSchemaToRow() throws Exception {
		byte[] bytes = new ProtoBufRowDeserializationSchemaTest().messageToBytes();

		DynamicMessage dynamicMessage = DynamicMessage.parseFrom(Message.Log.getDescriptor(), bytes);

		Map<Descriptors.FieldDescriptor, Object> allFields = dynamicMessage.getAllFields();
		Descriptors.Descriptor descriptorForType = dynamicMessage.getDescriptorForType();

		System.out.println("name");

	}

	@Test
	public void convertSchemaToObjectArray() throws Exception {

	}

}

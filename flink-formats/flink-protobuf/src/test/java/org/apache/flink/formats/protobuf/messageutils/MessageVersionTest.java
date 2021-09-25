package org.apache.flink.formats.protobuf.messageutils;

import org.apache.flink.formats.protobuf.typeutils.MessageVersion;
import org.junit.Test;

/**
 * Created by liufangliang on 2020/6/5.
 */
public class MessageVersionTest {

	@Test
	public void testMessageVersion(){

		MessageVersion messageV1 = MessageVersion.MESSAGE_V1;
		System.out.println(messageV1);
	}

}

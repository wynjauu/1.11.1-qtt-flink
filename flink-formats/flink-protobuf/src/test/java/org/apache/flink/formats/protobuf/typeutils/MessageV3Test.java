package org.apache.flink.formats.protobuf.typeutils;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.protobuf.typeutils.MessageVersion;
import org.apache.flink.formats.protobuf.typeutils.TypeInformationUtil;
import org.junit.Test;

/**
 * Created by liufangliang on 2020/6/11.
 */
public class MessageV3Test {
	@Test
	public void getFullTypeInfo() throws Exception {


		String messageV1 = MessageVersion.MESSAGE_V1.getVersion();
		String messageV3 = MessageVersion.MESSAGE_V2.getVersion();

		RowTypeInfo of = TypeInformationUtil.of(messageV3);

		System.out.println(of);

	}

}

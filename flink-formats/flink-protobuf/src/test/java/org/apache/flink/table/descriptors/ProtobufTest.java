package org.apache.flink.table.descriptors;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.descriptors.Protobuf;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by liufangliang on 2020/6/1.
 */
public class ProtobufTest {


	@Test
	public void testProtobuf() {
		TypeInformation<Row> information = Types.ROW_NAMED(
			new String[]{"protocol_version", "event_time", "arrive_time"},
			new TypeInformation[]{Types.STRING, Types.LONG, Types.LONG});

		String messageVersion = "message-v3";
		boolean ignoreParseErrors = false;
		Protobuf protobuf = new Protobuf().setTypeInformation(information)
			.setQttMessageVersion(messageVersion)
			.setIgnoreParseErrors(ignoreParseErrors);

		Map<String, String> formatProperties = protobuf.toFormatProperties();
		Map<String, String> allProperties = protobuf.toProperties();

		assertEquals(formatProperties, allProperties);
	}


}

package org.apache.flink.formats.protobuf;

import org.apache.flink.formats.protobuf.typeutils.MessageVersion;
import org.junit.Test;

import static org.apache.flink.formats.protobuf.ProtoBufOptions.QUTOUTIAO_MESSAGE_VERSION;

/**
 * Created by liufangliang on 2020/8/19.
 */
public class ProtoBufFormatFactoryTest {
	@Test
	public void createEncodingFormat() throws Exception {

	}

	@Test
	public void createDecodingFormat() throws Exception {

	}

	@Test
	public void factoryIdentifier() throws Exception {

	}


	@Test
	public void testValidation() throws Exception {
		String message = String.format(
			"Unsupported value '%s' for '%s'. Supported values are %s.",
			"message",
			QUTOUTIAO_MESSAGE_VERSION.key(),
			MessageVersion.getVersions()
		);
		System.out.println(message);
	}
}

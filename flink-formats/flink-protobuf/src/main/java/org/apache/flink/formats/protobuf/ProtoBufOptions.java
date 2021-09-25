package org.apache.flink.formats.protobuf;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.formats.protobuf.typeutils.MessageVersion;

/**
 * @author liufangliang
 * @date 2020/6/1 5:31 PM
 */

public class ProtoBufOptions {

	public static ConfigOption<Boolean> IGNORE_PARSE_ERRORS = ConfigOptions.key("ignore-parse-errors")
		.booleanType()
		.defaultValue(false)
		.withDescription("ignore-parse-errors, default false. ");

	public static ConfigOption<String> QUTOUTIAO_MESSAGE_VERSION = ConfigOptions.key("qutoutiao-message-version")
		.stringType()
		.noDefaultValue()
		.withDescription(String.format("qutoutiao-message-version. Required, must in %s", MessageVersion.getVersions()));

}

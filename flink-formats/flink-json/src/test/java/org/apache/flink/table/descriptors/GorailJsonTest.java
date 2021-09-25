/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.descriptors;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.ValidationException;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the {@link Json} descriptor.
 */
public class GorailJsonTest {

	private static final String JSON_SCHEMA = "{\n" +
		"    \"timestamp\":1617777063,\n" +
		"    \"gorail_nano\":1617777063704087734,\n" +
		"    \"action\":\"insert\",\n" +
		"    \"schema\":\"withdraw\",\n" +
		"    \"table\":\"order_20210407\",\n" +
		"    \"rows\":[\n" +
		"        {\n" +
		"            \"balance\":300,\n" +
		"            \"bonus_point\":3000,\n" +
		"            \"cancel_time\":\"0000-00-00 00:00:00\",\n" +
		"            \"check_time\":\"0000-00-00 00:00:00\",\n" +
		"            \"create_time\":\"2021-04-07 14:31:03\",\n" +
		"            \"deal_message\":\"\",\n" +
		"            \"deny_time\":\"0000-00-00 00:00:00\",\n" +
		"            \"id\":1429567,\n" +
		"            \"member_id\":1267710680,\n" +
		"            \"order_amount\":300,\n" +
		"            \"order_bid\":\"20210407143103019029004096868368\",\n" +
		"            \"order_id\":\"20210407143103019029004096868368\",\n" +
		"            \"order_num\":1,\n" +
		"            \"pay_balance\":300,\n" +
		"            \"pay_bonus_point\":3000,\n" +
		"            \"pay_time\":\"0000-00-00 00:00:00\",\n" +
		"            \"product_id\":29,\n" +
		"            \"refund_time\":\"0000-00-00 00:00:00\",\n" +
		"            \"remark\":\"\",\n" +
		"            \"sell_id\":200509,\n" +
		"            \"sell_snapshot_id\":0,\n" +
		"            \"ship_time\":\"0000-00-00 00:00:00\",\n" +
		"            \"source_type\":0,\n" +
		"            \"status\":40,\n" +
		"            \"tp_way\":20000,\n" +
		"            \"update_time\":\"0000-00-00 00:00:00\"\n" +
		"        }\n" +
		"    ],\n" +
		"    \"raw_rows\":[\n" +
		"\n" +
		"    ],\n" +
		"    \"primary_keys\":[\n" +
		"        [\n" +
		"            1429567\n" +
		"        ]\n" +
		"    ]\n" +
		"}";

	@Test
	public void test() throws IOException {
		final ObjectMapper objectMapper = new ObjectMapper();
		final JsonNode root = objectMapper.readTree(JSON_SCHEMA.getBytes(StandardCharsets.UTF_8));
	}
}

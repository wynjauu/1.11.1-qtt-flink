package org.apache.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * @author: zhushang
 * @create: 2020-08-27 00:21
 **/

public class SinkToJDBC {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tbEnv = StreamTableEnvironment.create(env, settings);
		tbEnv.executeSql("CREATE TABLE user_behavior (\n" +
			"    user_id BIGINT,\n" +
			"    item_id BIGINT,\n" +
			"    category_id BIGINT,\n" +
			"    behavior STRING,\n" +
			"    ts TIMESTAMP(3),\n" +
			"    proctime as PROCTIME(),   -- 通过计算列产生一个处理时间列\n" +
			"    WATERMARK FOR ts as ts - INTERVAL '5' SECOND  -- 在ts上定义watermark，ts成为事件时间列\n" +
			") WITH (\n" +
			"    'connector.type' = 'kafka',  -- 使用 kafka connector\n" +
			"    'connector.version' = 'universal',  -- kafka 版本，universal 支持 0.11 以上的版本\n" +
			"    'connector.topic' = 'user_behavior',  -- kafka topic\n" +
			"    'connector.startup-mode' = 'earliest-offset',  -- 从起始 offset 开始读取\n" +
//                "    'connector.properties.group.id' = ''"+
			"    'connector.properties.zookeeper.connect' = 'localhost:2181',  -- zookeeper 地址\n" +
			"    'connector.properties.bootstrap.servers' = 'localhost:9092',  -- kafka broker 地址\n" +
			"    'format.type' = 'json'  -- 数据源格式为 json\n" +
			")");
		tbEnv.executeSql("CREATE TABLE jdbctable (\n" +
			"  user_id BIGINT,\n" +
			"  item_id BIGINT\n" +
			") WITH (\n" +
			"   'connector' = 'jdbc',\n" +
			"   'url' = 'jdbc:mysql://localhost:3306/flink',\n" +
			"   'table-name' = 'jdbctable'\n" +
			")");
		tbEnv.executeSql("insert into jdbctable select user_id,item_id from user_behavior");
    }
}

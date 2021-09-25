package org.apache.flink.redis.test;

import org.apache.flink.redis.api.util.RedisConnectionUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * @author: zhushang
 * @create: 2020-08-31 17:07
 **/

public class RedisTest {
	@Test
	public void test(){
		JedisPool jedisPool = RedisConnectionUtils.getJedisPool(RedisConnectionUtils.getJedisPoolConfig(), "10.104.34.225", 6379, "");
		Jedis jedis = jedisPool.getResource();
		System.out.println(jedis.get("pv"));
	}

	public static void main(String[] args)
	{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
		StreamTableEnvironment tbEnv = StreamTableEnvironment.create(env, settings);
		tbEnv.executeSql("CREATE TABLE kafka_source (\n" +
			"    user_id BIGINT,\n" +
			"    item_id BIGINT,\n" +
			"    category_id BIGINT,\n" +
			"    behavior STRING,\n" +
			"    ts BIGINT,\n" +
			"    proctime as PROCTIME()\n" +
			") WITH (\n" +
			"    'connector.type' = 'kafka',  -- 使用 kafka connector\n" +
			"    'connector.version' = 'universal',  -- kafka 版本，universal 支持 0.11 以上的版本\n" +
			"    'connector.topic' = 'test4',  -- kafka topic\n" +
			"    'connector.startup-mode' = 'earliest-offset',  -- 从起始 offset 开始读取\n" +
			"    'connector.properties.zookeeper.connect' = 'localhost:2181',  -- zookeeper 地址\n" +
			"    'connector.properties.bootstrap.servers' = 'localhost:9092',  -- kafka broker 地址\n" +
			"    'format.type' = 'json', -- 数据源格式为 json\n" +
			"    --'keyby.index' = '3',\n" +
			"    'delay.open' = 'false',\n" +
			"    'delay.second' ='100',\n" +
			"    'delay.eventtime.field.index' = '4',\n" +
			"    'delay.cache.size' ='1000',\n" +
			"    'parallelism' = '2'\n" +
			")");
		tbEnv.executeSql("create table redis_dim (\n" +
			"    key STRING,\n" +
			"    `value` STRING\n" +
			") with (\n" +
			"    'connector' = 'redis',\n" +
			"    'ip' = '10.104.34.225',\n" +
			"    'port' = '6379',\n" +
			"    'password' = '',\n" +
			"    'type' = 'string',\n" +
			"    'open.cache' = 'false',--是否开启LRU缓存\n" +
			"    'cache.size' = '10000000',--缓存大小\n" +
			"    'write.open' = 'false', --是否开启关联时写入\n" +
			"    'store.key.strategy' = 'always',--写入策略，always：始终写入 if_not_exist：不存在时写入\n" +
			"    'key.append.character' = '|' --key连接符\n" +
			")");

		tbEnv.executeSql("create table print_sink(user_id BIGINT,behavior String,v String) with ('connector' = 'print')");

		tbEnv.executeSql("insert into print_sink select k.user_id,k.behavior,r.`value` from kafka_source k " +
			"left join redis_dim FOR SYSTEM_TIME AS OF k.proctime r on k.behavior=r.key");
	}
}

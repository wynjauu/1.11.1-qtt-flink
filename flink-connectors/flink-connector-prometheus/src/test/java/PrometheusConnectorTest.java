import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PrometheusConnectorTest {
	@Test
	public void secondFieldTest() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
		tableEnv.sqlUpdate(getPrometheusTable_2());
		tableEnv.sqlUpdate("insert into prometheus_table select 'zs_key_2' as `group`,cast(77.0 as double) as `value`");
		env.execute();
	}

	private String getPrometheusTable_2() {
		String ddl = "CREATE TABLE prometheus_table (" +
			"`group`  STRING," +
			" `value` DOUBLE)" +
			"WITH (" +
			"'connector.type' = 'prometheus'," +
			"'connector.job' = 'mytestJob'," +
			"'connector.metrics' = 'mytestMetrics'," +
			"'connector.address' = 'localhost:9091'" +
			")";
		//CREATE TABLE prometheus_table2 (`group` STRING,`value` DOUBLE )WITH ('connector.type' = 'prometheus','connector.job' = 'testJob','connector.metrics' = 'testMetrics','connector.address' = 'localhost:9091')
		return ddl;
	}

	@Test
	public void oneFieldTest() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
		tableEnv.sqlUpdate(getPrometheusTable_1());
		tableEnv.sqlUpdate("insert into prometheus_table select cast(5.01 as double) as `value`");
		env.execute();
	}

	private String getPrometheusTable_1() {
		String ddl = "CREATE TABLE prometheus_table (" +
			"`value`  DOUBLE)" +
			"WITH (" +
			"'connector.type' = 'prometheus'," +
			"'connector.job' = 'mytestJob'," +
			"'connector.metrics' = 'mytestMetrics'," +
			"'connector.address' = '10.104.34.225:9091'" +
			")";
		return ddl;
	}

	private String getPrometheusTable_withlabel() {
		String ddl = "CREATE TABLE prometheus_table (" +
			"job STRING,"+
			"metrics STRING,"+
			"labels MAP<STRING,STRING>,"+
			"`value`  DOUBLE" +
			")" +
			"WITH (" +
			"'connector.type' = 'prometheus'," +
			"'connector.address' = '10.104.34.225:9091'" +
			")";
		return ddl;
	}

	private String getPrometheusTable_nolabel() {
		String ddl = "CREATE TABLE prometheus_table (" +
			"j STRING,"+
			"metrics STRING,"+
			"`value`  DOUBLE" +
			")" +
			"WITH (" +
			"'connector.type' = 'prometheus'," +
			"'connector.address' = '10.104.34.225:9091'" +
			")";
		return ddl;
	}

	@Test
	public void version2WithLabelsTest() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
		tableEnv.sqlUpdate(getPrometheusTable_withlabel());
		tableEnv.sqlUpdate("insert into prometheus_table select 'job2_zs' as j,'metrics_zs' as metrics,Map['label1','l1','lable2','l2'] as labels,cast(11.01 as double) as `value`");
		env.execute();
	}
	@Test
	public void version2NoLabelsTest() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
		tableEnv.sqlUpdate(getPrometheusTable_nolabel());
		tableEnv.sqlUpdate("insert into prometheus_table select 'job1_zs' as job,'metrics1_zs' as metrics,cast(1.01 as double) as `value`");
		env.execute();
	}
	@Test
	public void dynamic() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
		tableEnv.executeSql(dynamicTable());
		tableEnv.executeSql("insert into prometheus_table select 'job_new' as job,'metrics' as metrics,Map['user','u','behavior','b'] as labels,cast(1093472534 as double)/1000 as `value`");
	}

	private String dynamicTable() {
		return "CREATE TABLE prometheus_table (\n" +
			"\t\t\tjob STRING,\n" +
			"\t\t\tmetrics STRING,\n" +
			"\t\t\tlabels MAP<STRING,STRING>,\n" +
			"\t\t\t`value` DOUBLE \n" +
			") WITH (\n" +
			"\t\t\t'connector' = 'prometheus',\n" +
			"\t\t\t'address' = '10.104.34.225:9091'\n" +
			")";
	}






}

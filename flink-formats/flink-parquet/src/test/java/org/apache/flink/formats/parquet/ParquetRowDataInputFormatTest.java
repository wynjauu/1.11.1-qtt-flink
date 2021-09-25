package org.apache.flink.formats.parquet;

import net.qutoutiao.dataplatform.model.PB3Avro;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: zhushang
 * @create: 2020-12-23 14:24
 **/

public class ParquetRowDataInputFormatTest {

	@Test
	public void testConvert(){
		MessageType messageType = new AvroSchemaConverter().convert(PB3Avro.SCHEMA$);
		ParquetRowDataInputFormat format = new ParquetRowDataInputFormat(null, messageType);
		Row row = new Row(50);
		row.setField(0,null);
		row.setField(1,null);
		row.setField(2,null);
		row.setField(3,null);
		row.setField(4,null);
		row.setField(5,null);
		row.setField(6,null);
		row.setField(7,null);
		row.setField(8,null);
		row.setField(9,null);
		row.setField(10,null);
		row.setField(11,null);
		row.setField(12,null);
		row.setField(13,null);
		row.setField(14,null);
		row.setField(15,null);
		row.setField(16,null);
		row.setField(17,null);
		row.setField(18,null);
		row.setField(19,null);
		row.setField(20,null);
		row.setField(21,null);
		row.setField(22,null);
		row.setField(23,null);
		row.setField(24,null);
		row.setField(25,null);
		row.setField(26,null);
		row.setField(27,null);
		row.setField(28,null);
		row.setField(29,null);
		row.setField(30,null);
		row.setField(31,null);
		row.setField(32,null);
		row.setField(33,null);
		row.setField(34,1608704217549L);
		row.setField(35,"app_alive_time");
		row.setField(36,null);
		row.setField(37,null);
		row.setField(38,null);
		row.setField(39,null);
		row.setField(40,null);
		row.setField(41,null);
		row.setField(42,null);
		row.setField(43,null);
		row.setField(44,null);
		row.setField(45,null);
		row.setField(46,null);
		row.setField(47,null);
		row.setField(48,null);
		Map<String,String> map = new HashMap<>();
		map.put("market","qtt");
		map.put("shell_v","20009000");
		map.put("engine_type","native");
		map.put("gr_v","");
		map.put("start_id","-211800950");
		map.put("gf_v","");
		map.put("orgtk","ACFDWay8puYvw-0xrtAW0kGX7SgT7aO_iFlnbXd5eW14Mg");
		map.put("app_id","a45eLzxGxDEY");
		map.put("orgtuid","Q1msvKbmL8PtMa7QFtJBlw");
		row.setField(49,map);
		RowData rowData = format.convert(row);
	}
}

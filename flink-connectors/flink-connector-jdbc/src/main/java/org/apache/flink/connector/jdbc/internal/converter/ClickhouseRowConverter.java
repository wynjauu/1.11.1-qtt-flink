package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.table.types.logical.RowType;

/**
 * @author: zhushang
 * @create: 2020-09-01 15:54
 **/

public class ClickhouseRowConverter extends AbstractJdbcRowConverter{
	@Override
	public String converterName() {
		return "ClickhouseSQL";
	}
	public ClickhouseRowConverter(RowType rowType) {
		super(rowType);
	}
}

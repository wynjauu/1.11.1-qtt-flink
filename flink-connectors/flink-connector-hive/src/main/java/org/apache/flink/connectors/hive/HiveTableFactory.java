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

package org.apache.flink.connectors.hive;

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;

import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A table factory implementation for Hive catalog.
 */
public class HiveTableFactory
	implements TableSourceFactory<RowData>, TableSinkFactory {

	private final HiveConf hiveConf;

	public HiveTableFactory(HiveConf hiveConf) {
		this.hiveConf = checkNotNull(hiveConf, "hiveConf cannot be null");
	}

	@Override
	public Map<String, String> requiredContext() {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<String> supportedProperties() {
		throw new UnsupportedOperationException();
	}

	@Override
	public TableSource<RowData> createTableSource(TableSourceFactory.Context context) {
		CatalogTable table = checkNotNull(context.getTable());
		Preconditions.checkArgument(table instanceof CatalogTableImpl);

		boolean isGeneric = Boolean.parseBoolean(table.getProperties().get(CatalogConfig.IS_GENERIC));

		if (!isGeneric) {
			return new HiveTableSource(
				new JobConf(hiveConf),
				context.getConfiguration(),
				context.getObjectIdentifier().toObjectPath(),
				table);
		} else {
			return TableFactoryUtil.findAndCreateTableSource(context);
		}
	}

	@Override
	public TableSink createTableSink(TableSinkFactory.Context context) {
		CatalogTable table = checkNotNull(context.getTable());
		Preconditions.checkArgument(table instanceof CatalogTableImpl);

		boolean isGeneric = Boolean.parseBoolean(table.getProperties().get(CatalogConfig.IS_GENERIC));

		if (!isGeneric) {
			return new HiveTableSink(
				context.getConfiguration(),
				context.isBounded(),
				new JobConf(hiveConf),
				context.getObjectIdentifier(),
				table);
		} else {
			return TableFactoryUtil.findAndCreateTableSink(context);
		}
	}
}

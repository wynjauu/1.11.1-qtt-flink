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

package org.apache.flink.formats.json.gorail;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Objects;

import static java.lang.String.format;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Deserialization schema from gorail JSON to Flink Table/SQL internal data structure {@link RowData}.
 * The deserialization schema knows gorail's schema definition and can extract the database data
 * and convert into {@link RowData} with {@link RowKind}.
 *
 * <p>Deserializes a <code>byte[]</code> message as a JSON object and reads
 * the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 *
 * @see <a href="https://github.com/tenfer/gorail">qutoutiao.net gorail</a>
 */
public final class GorailJsonDeserializationSchema implements DeserializationSchema<RowData> {
	private static final long serialVersionUID = 1L;

	private static final String OP_INSERT = "insert";
	private static final String OP_UPDATE = "update";
	private static final String OP_DELETE = "delete";

	/**
	 * The deserializer to deserialize gorail JSON data.
	 */
	private final JsonRowDataDeserializationSchema jsonDeserializer;

	/**
	 * TypeInformation of the produced {@link RowData}.
	 **/
	private final TypeInformation<RowData> resultTypeInfo;

	/**
	 * Flag indicating whether to ignore invalid fields/rows (default: throw an exception).
	 */
	private final boolean ignoreParseErrors;

	/**
	 * Number of fields.
	 */
	private final int fieldCount;

	public GorailJsonDeserializationSchema(
		RowType rowType,
		TypeInformation<RowData> resultTypeInfo,
		boolean ignoreParseErrors,
		TimestampFormat timestampFormatOption) {
		this.resultTypeInfo = resultTypeInfo;
		this.ignoreParseErrors = ignoreParseErrors;
		this.fieldCount = rowType.getFieldCount();
		this.jsonDeserializer = new JsonRowDataDeserializationSchema(
			createJsonRowType(fromLogicalToDataType(rowType)),
			// the result type is never used, so it's fine to pass in gorail's result type
			resultTypeInfo,
			false, // ignoreParseErrors already contains the functionality of failOnMissingField
			ignoreParseErrors,
			timestampFormatOption);

	}

	@Override
	public RowData deserialize(byte[] message) throws IOException {
		throw new RuntimeException(
			"Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
	}

	@Override
	public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
		try {
			RowData row = jsonDeserializer.deserialize(message);
			String action = row.getString(2).toString(); // "action" field
			if (OP_INSERT.equals(action)) {
				row.setRowKind(RowKind.INSERT);
				out.collect(row);
			} else if (OP_UPDATE.equals(action)) {
				row.setRowKind(RowKind.UPDATE_AFTER);
				out.collect(row);
			} else if (OP_DELETE.equals(action)) {
				row.setRowKind(RowKind.DELETE);
				out.collect(row);
			} else {
				if (!ignoreParseErrors) {
					throw new IOException(format(
						"Unknown \"action\" value \"%s\". The gorail JSON message is '%s'", action, new String(message)));
				}
			}
		} catch (Throwable t) {
			// a big try catch to protect the processing.
			if (!ignoreParseErrors) {
				throw new IOException(format(
					"Corrupt gorail JSON message '%s'.", new String(message)), t);
			}
		}
	}

	@Override
	public boolean isEndOfStream(RowData nextElement) {
		return false;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return resultTypeInfo;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		GorailJsonDeserializationSchema that = (GorailJsonDeserializationSchema) o;
		return ignoreParseErrors == that.ignoreParseErrors &&
			fieldCount == that.fieldCount &&
			Objects.equals(jsonDeserializer, that.jsonDeserializer) &&
			Objects.equals(resultTypeInfo, that.resultTypeInfo);
	}

	@Override
	public int hashCode() {
		return Objects.hash(jsonDeserializer, resultTypeInfo, ignoreParseErrors, fieldCount);
	}

	private static RowType createJsonRowType(DataType databaseSchema) {
		return (RowType) databaseSchema.getLogicalType();

		// gorail JSON contains other information, e.g. "database", "ts"
		// but we don't need them
//		return (RowType) DataTypes.ROW(
//			DataTypes.FIELD("timestamp", DataTypes.BIGINT()),
//			DataTypes.FIELD("gorail_nano", DataTypes.STRING()),
//			DataTypes.FIELD("action", DataTypes.STRING()),
//			DataTypes.FIELD("schema", DataTypes.STRING()),
//			DataTypes.FIELD("table", DataTypes.STRING()),
//			DataTypes.FIELD("rows", DataTypes.ARRAY(databaseSchema)),
//			DataTypes.FIELD("raw_rows", DataTypes.ARRAY(databaseSchema)),
//			DataTypes.FIELD("primary_keys", DataTypes.ARRAY(DataTypes.BIGINT()))
//		).getLogicalType();
	}
}

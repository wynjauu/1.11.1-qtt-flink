package org.apache.flink.formats.protobuf.messageutils;

import com.google.protobuf.Descriptors;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.protobuf.exception.ProtobufException;
import org.apache.flink.formats.protobuf.message.Message;
import org.apache.flink.formats.protobuf.message.OptionalMessage;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liufangliang
 * @date 2020/6/10 7:32 PM
 */
public class MessageFieldEtlConverter extends ProtobufFieldConverter {
    public static final Logger LOGGER = LoggerFactory.getLogger(MessageFieldEtlConverter.class);

    private static final long serialVersionUID = -224294630388809195L;

    @Override
    String initializeSchemaString() {
        return "package pbdata;\n"
                + " \n"
                + "message ValueType {\n"
                + "    optional int32 int_type =1;\n"
                + "    optional int64 long_type = 2;\n"
                + "    optional float float_type = 3;\n"
                + "    optional string string_type =4;\n"
                + "}\n"
                + " \n"
                + "message Log {\n"
                + "    required int64 log_timestamp =1;\n"
                + "    optional string ip = 2;\n"
                + "    optional group Field  = 3 {\n"
                + "        repeated group Map = 1 {\n"
                + "            required string key = 1;\n"
                + "            optional ValueType value = 2;\n"
                + "        }\n"
                + "    }\n"
                + "}";
    }

    @Override
    Map<String, Descriptors.FieldDescriptor> initializeFieldDescriptors() {
        List<Descriptors.FieldDescriptor> fields = Message.Log.getDescriptor().getFields();
        Map<String, Descriptors.FieldDescriptor> descriptorMap = new HashMap<>(fields.size());
        for (Descriptors.FieldDescriptor field : fields) {
            String name = field.toProto().getName();
            descriptorMap.put(name, field);
        }
        return descriptorMap;
    }

    public MessageFieldEtlConverter(RowTypeInfo typeInformation, Boolean ignoreParseErrors) {
        super(typeInformation, ignoreParseErrors);
    }

    @Override
    Row convertSchemaToRow(byte[] message) {
        return null;
    }

    @Override
    Object[] convertSchemaToObjectArray(byte[] message) {
        return createFieldValues(message);
    }

    private Object[] createFieldValues(byte[] message) {
        try {
            Message.Log log = Message.Log.parseFrom(message);
            Object[] objects = new Object[protoFieldNumbers.length];
            for (int i = 0; i < this.protoFieldNumbers.length; i++) {
                objects[i] = getFieldValue(log, protoFieldNumbers[i]);
            }
            return objects;
        } catch (Exception e) {
            try {
                OptionalMessage.Log log = OptionalMessage.Log.parseFrom(message);
                Object[] objects = new Object[protoFieldNumbers.length];
                for (int i = 0; i < this.protoFieldNumbers.length; i++) {
                    objects[i] = getOptionalFieldValue(log, protoFieldNumbers[i]);
                }
                LOGGER.info("parse message exception,the message is [{}].", objects);
            } catch (Exception e1) {
                LOGGER.info(
                        "parse message exception,the message is [{}].", Arrays.toString(message));
            }
            if (!ignoreParseErrors) {
                throw new ProtobufException("parse row exception ", e);
            }
        }
        return null;
    }

    private Object getFieldValue(Message.Log log, int columnNumber) throws Exception {
        if (log == null) {
            return null;
        }
        switch (columnNumber) {
            case Message.Log.LOG_TIMESTAMP_FIELD_NUMBER:
                return log.getLogTimestamp();
            case Message.Log.IP_FIELD_NUMBER:
                return log.getIp();
            case Message.Log.FIELD_FIELD_NUMBER:
                return createMapFromField(log.getField());
            default:
                return null;
        }
    }

    private Map<String, Map<String, String>> createMapFromField(Message.Log.Field field) {
        Map<String, Map<String, String>> resultMap = new HashMap<>();

        if (field != null) {
            for (Message.Log.Field.Map map : field.getMapList()) {
                Message.ValueType mapValue = map.getValue();
                String key = map.getKey();
                String value = "";
                String type = "STRING";
                if (mapValue.hasStringType()) {
                    value = String.valueOf(mapValue.getStringType());
                    type = "STRING";
                } else if (mapValue.hasLongType()) {
                    value = String.valueOf(mapValue.getLongType());
                    type = "LONG";
                } else if (mapValue.hasFloatType()) {
                    value = String.valueOf(mapValue.getFloatType());
                    type = "FLOAT";
                } else {
                    value = String.valueOf(mapValue.getIntType());
                    type = "INTEGER";
                }
                HashMap<String, String> objectObjectHashMap = new HashMap<>();
                objectObjectHashMap.put("value", value);
                objectObjectHashMap.put("java_type", type);
                resultMap.put(key, objectObjectHashMap);
            }
        }
        return resultMap;
    }

    private Object getOptionalFieldValue(OptionalMessage.Log log, int columnNumber)
            throws Exception {
        if (log == null) {
            return null;
        }
        switch (columnNumber) {
            case OptionalMessage.Log.LOG_TIMESTAMP_FIELD_NUMBER:
                return log.getLogTimestamp();
            case OptionalMessage.Log.IP_FIELD_NUMBER:
                return log.getIp();
            case OptionalMessage.Log.FIELD_FIELD_NUMBER:
                return createOptionalMapFromField(log.getField());
            default:
                return null;
        }
    }

    private Map<String, Map<String, String>> createOptionalMapFromField(
            OptionalMessage.Log.Field field) {
        Map<String, Map<String, String>> resultMap = new HashMap<>();

        if (field != null) {
            for (OptionalMessage.Log.Field.Map map : field.getMapList()) {
                OptionalMessage.ValueType mapValue = map.getValue();
                String key = map.getKey();
                String value = "";
                String type = "STRING";
                if (mapValue.hasStringType()) {
                    value = String.valueOf(mapValue.getStringType());
                    type = "STRING";
                } else if (mapValue.hasLongType()) {
                    value = String.valueOf(mapValue.getLongType());
                    type = "LONG";
                } else if (mapValue.hasFloatType()) {
                    value = String.valueOf(mapValue.getFloatType());
                    type = "FLOAT";
                } else {
                    value = String.valueOf(mapValue.getIntType());
                    type = "INTEGER";
                }
                HashMap<String, String> objectObjectHashMap = new HashMap<>();
                objectObjectHashMap.put("value", value);
                objectObjectHashMap.put("java_type", type);
                resultMap.put(key, objectObjectHashMap);
            }
        }
        return resultMap;
    }
}

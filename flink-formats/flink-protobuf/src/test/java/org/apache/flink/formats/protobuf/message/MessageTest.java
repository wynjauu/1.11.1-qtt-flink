package org.apache.flink.formats.protobuf.message;

import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

/** Created by liufangliang on 2020/6/5. */
public class MessageTest {

    @Test
    public void testMessageSchema() throws Exception {
        Message.Log.Builder builder = Message.Log.newBuilder();
        builder.setIp("172.0.0.1");
        builder.setLogTimestamp(123433L);

        Message.Log.Field.Builder field = Message.Log.Field.newBuilder();
        for (int i = 0; i < 10; i++) {
            Message.Log.Field.Map.Builder map = Message.Log.Field.Map.newBuilder();
            map.setKey("key" + i);
            map.setValue(Message.ValueType.newBuilder().setIntType(i));
            field.addMap(map);
        }
        builder.setField(field);

        Message.Log build = builder.build();
        System.out.println(build.toString());
    }

    @Test
    public void testMessageOption() throws InvalidProtocolBufferException {
        Message.Log.Builder builder = Message.Log.newBuilder();
        builder.setIp("172.0.0.1");
        builder.setLogTimestamp(123433L);

        Message.Log.Field.Builder field = Message.Log.Field.newBuilder();
        for (int i = 0; i < 10; i++) {
            Message.Log.Field.Map.Builder map = Message.Log.Field.Map.newBuilder();
            map.setKey("key" + i);
            map.setValue(Message.ValueType.newBuilder().setIntType(i));
            field.addMap(map);
        }
        builder.setField(field);

        Message.Log build = builder.build();
        OptionalMessage.Log log = OptionalMessage.Log.parseFrom(build.toByteArray());
        System.out.println(log.getLogTimestamp());
        System.out.println(log.getIp());
        List<Integer> collect =
                log.getField().getMapList().stream()
                        .map(
                                map -> {
                                    if (map.getValue().hasIntType())
                                        return map.getValue().getIntType();
                                    else return -1;
                                })
                        .collect(Collectors.toList());
        System.out.println(collect);
    }
}

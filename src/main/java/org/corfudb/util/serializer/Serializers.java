package org.corfudb.util.serializer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by mwei on 1/8/16.
 */

@Slf4j
public class Serializers {

    public static final ISerializer CORFU = new CorfuSerializer((byte) 0);
    public static final ISerializer JAVA = new JavaSerializer((byte) 1);
    public static final ISerializer JSON = new JSONSerializer((byte) 2);
    public static final ISerializer PRIMITIVE = new PrimitiveSerializer((byte) 3);

    private static final Map<Byte, ISerializer> serializersMap;
    static
    {
        serializersMap = new HashMap();
        serializersMap.put(CORFU.getType(), CORFU);
        serializersMap.put(JAVA.getType(), JAVA);
        serializersMap.put(JSON.getType(), JSON);
        serializersMap.put(PRIMITIVE.getType(), PRIMITIVE);
    }

    public static ISerializer getSerializer(Byte type) {
        return serializersMap.get(type);
    }
}

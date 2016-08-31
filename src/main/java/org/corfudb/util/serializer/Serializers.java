package org.corfudb.util.serializer;

import com.google.common.collect.ImmutableMap;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by mwei on 1/8/16.
 */
public class Serializers {

    public static final SerializerType CORFU = new SerializerType(CorfuSerializer.class, "CORFU");
    public static final SerializerType JAVA = new SerializerType(JavaSerializer.class, "JAVA");
    public static final SerializerType JSON = new SerializerType(JSONSerializer.class, "JSON");
    public static final SerializerType PRIMITIVE = new SerializerType(PrimitiveSerializer.class, "PRIMITIVE");

    public static final Map<String, SerializerType> typeMap = new HashMap() {{
        put(CORFU.getTypeName(), CORFU);
        put(JAVA.getTypeName(), JAVA);
        put(JSON.getTypeName(), JSON);
        put(PRIMITIVE.getTypeName(), PRIMITIVE);
    }};

    public static synchronized void registerSerializer(SerializerType type) {
        typeMap.put(type.getTypeName(), type);
    }

    public static final Map<SerializerType, ISerializer> serializerCache = new HashMap<>();

    public static ISerializer getSerializer(SerializerType type) {
        return serializerCache.computeIfAbsent(type, x -> {
            try {
                return type.entryType.newInstance();
            } catch (InstantiationException | IllegalAccessException ie) {
                throw new RuntimeException(ie);
            }
        });
    }

}

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
    public static final Map<Byte, SerializerType> typeMap =
            Arrays.stream(SerializerType.values())
                    .collect(Collectors.toMap(SerializerType::asByte, Function.identity()));


    public static final Map<SerializerType, ISerializer> serializerCache = new HashMap<>();

    public static synchronized void setCustomSerializer (Class<? extends ISerializer> serializer) {
        SerializerType.CUSTOM.setSerializer(serializer);
    }

    public static ISerializer getSerializer(SerializerType type) {
        return serializerCache.computeIfAbsent(type, x -> {
            try {
                return type.getSerializer().newInstance();
            } catch (InstantiationException | IllegalAccessException ie) {
                log.error("Error creating a serializer for {}", type.name());
                throw new RuntimeException(ie);
            }
        });
    }
}

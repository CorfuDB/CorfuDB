package org.corfudb.util.serializer;

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
    public static final Map<Byte, SerializerType> typeMap =
            Arrays.stream(SerializerType.values())
                    .collect(Collectors.toMap(SerializerType::asByte, Function.identity()));

    ;
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

    @RequiredArgsConstructor
    public enum SerializerType {
        // Supported Serializers
        CORFU(0, CorfuSerializer.class),
        JAVA(1, JavaSerializer.class),
        JSON(2, JSONSerializer.class),
        PRIMITIVE(3, PrimitiveSerializer.class);

        public final int type;
        public final Class<? extends ISerializer> entryType;

        public byte asByte() {
            return (byte) type;
        }
    }
}

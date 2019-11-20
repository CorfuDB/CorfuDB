package org.corfudb.util.serializer;

import java.util.HashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.SerializerException;

@Slf4j
public class Serializers {

    public static final int SYSTEM_SERIALIZERS_COUNT = 10;

    public static final ISerializer CORFU = new CorfuSerializer((byte) 0);
    public static final ISerializer JAVA = new JavaSerializer((byte) 1);
    public static final ISerializer JSON = new JsonSerializer((byte) 2);
    public static final ISerializer PRIMITIVE = new PrimitiveSerializer((byte) 3);

    private Serializers() {
        //prevent creating instances
    }

    /**
     * @return the recommended default serializer used for converting objects into write format.
     */
    public static ISerializer getDefaultSerializer() {
        return Serializers.JSON;
    }

    private static final Map<Byte, ISerializer> CUSTOM_SERIALIZERS = new HashMap<>();

    private static final Map<Byte, ISerializer> SERIALIZERS_MAP = new HashMap<>();

    static {
        SERIALIZERS_MAP.put(CORFU.getType(), CORFU);
        SERIALIZERS_MAP.put(JAVA.getType(), JAVA);
        SERIALIZERS_MAP.put(JSON.getType(), JSON);
        SERIALIZERS_MAP.put(PRIMITIVE.getType(), PRIMITIVE);
    }

    /**
     * Return the serializer byte.
     * @param type A byte that tags a serializer
     * @return     A serializer that corresponds to the type byte
     */
    public static ISerializer getSerializer(Byte type) {
        if (type <= SYSTEM_SERIALIZERS_COUNT) {
            if (SERIALIZERS_MAP.containsKey(type)) {
                return SERIALIZERS_MAP.get(type);
            }
        } else if (CUSTOM_SERIALIZERS.containsKey(type)) {
            return CUSTOM_SERIALIZERS.get(type);
        }

        log.error("Serializer with type code {} not found. Please check custom serializers " +
                "for this client.", type.intValue());
        throw new SerializerException(type);
    }

    /**
     * Register a serializer.
     * @param serializer Serializer to register
     */
    public static synchronized void registerSerializer(ISerializer serializer) {
        if (serializer.getType() > SYSTEM_SERIALIZERS_COUNT) {
            CUSTOM_SERIALIZERS.put(serializer.getType(), serializer);
        } else {
            String msg = String.format(
                    "Serializer id must be greater than %s", SYSTEM_SERIALIZERS_COUNT);
            throw new IllegalStateException(msg);
        }
    }
}

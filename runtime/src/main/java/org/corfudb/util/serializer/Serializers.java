package org.corfudb.util.serializer;

import java.util.HashMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.SerializerException;


/**
 * Created by mwei on 1/8/16.
 */

@Slf4j
public class Serializers {

    public static final int SYSTEM_SERIALIZERS_COUNT = 10;
    public static final ISerializer CORFU = new CorfuSerializer((byte) 0);
    public static final ISerializer JAVA = new JavaSerializer((byte) 1);
    public static final ISerializer JSON = new JsonSerializer((byte) 2);
    public static final ISerializer PRIMITIVE = new PrimitiveSerializer((byte) 3);
    public static final ISerializer QUEUE_SERIALIZER = new CorfuQueueSerializer((byte) 4);


    /**
     * @return the recommended default serializer used for converting objects into write format.
     */
    public static final ISerializer getDefaultSerializer() {
        return Serializers.JSON;
    }

    private static final Map<Byte, ISerializer> serializersMap;

    static {
        serializersMap = new HashMap();
        serializersMap.put(CORFU.getType(), CORFU);
        serializersMap.put(JAVA.getType(), JAVA);
        serializersMap.put(JSON.getType(), JSON);
        serializersMap.put(PRIMITIVE.getType(), PRIMITIVE);
        serializersMap.put(QUEUE_SERIALIZER.getType(), QUEUE_SERIALIZER);
    }

    private static final Map<Byte, ISerializer> customSerializers = new HashMap<>();

    /**
     * Return the serializer byte.
     * @param type A byte that tags a serializer
     * @return     A serializer that corresponds to the type byte
     */
    public static ISerializer getSerializer(Byte type) {
        if (type <= SYSTEM_SERIALIZERS_COUNT) {
            if (serializersMap.containsKey(type)) {
                return serializersMap.get(type);
            }
        } else if (customSerializers.containsKey(type)) {
            return customSerializers.get(type);
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
            customSerializers.put(serializer.getType(), serializer);
        } else {
            String msg = String.format("Serializer id must be greater than {}",
                    SYSTEM_SERIALIZERS_COUNT);
            throw new RuntimeException(msg);
        }
    }

    /**
     * Clear custom serializers.
     */
    public static synchronized void clearCustomSerializers() {
        customSerializers.clear();
    }

    @VisibleForTesting
    public static synchronized void removeSerializer(ISerializer serializer) {
        customSerializers.remove(serializer.getType());
    }
}

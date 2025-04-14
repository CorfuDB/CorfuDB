package org.corfudb.util.serializer;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.SerializerException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * Created by mwei on 1/8/16.
 */

@Slf4j
public class Serializers {

    public static final int SYSTEM_SERIALIZERS_COUNT = 10;
    public static final CorfuSerializer CORFU = new CorfuSerializer((byte) 0);
    public static final JavaSerializer JAVA = new JavaSerializer((byte) 1);
    public static final JsonSerializer JSON = new JsonSerializer((byte) 2);
    public static final PrimitiveSerializer PRIMITIVE = new PrimitiveSerializer((byte) 3);
    public static final CorfuQueueSerializer QUEUE_SERIALIZER = new CorfuQueueSerializer((byte) 4);

    public static final Map<Byte, ISerializer> serializersMap;

    static {
        serializersMap = new HashMap<>();
        serializersMap.put(CORFU.getType(), CORFU);
        serializersMap.put(JAVA.getType(), JAVA);
        serializersMap.put(JSON.getType(), JSON);
        serializersMap.put(PRIMITIVE.getType(), PRIMITIVE);
        serializersMap.put(QUEUE_SERIALIZER.getType(), QUEUE_SERIALIZER);
    }

    private final ConcurrentMap<Byte, ISerializer> customSerializers = new ConcurrentHashMap<>();

    /**
     * @return the recommended default serializer used for converting objects into write format.
     */
    public static ISerializer getDefaultSerializer() {
        return Serializers.JSON;
    }

    public <T extends ISerializer> T getSerializer(Byte type, Class<T> serializerType) {
        ISerializer serializer = getSerializer(type);
        if (serializerType.isInstance(serializer)) {
            return serializerType.cast(serializer);
        } else {
            throw new IllegalArgumentException("The serializer cannot be cast to " + serializerType.getName());
        }
    }

    /**
     * Return the serializer byte.
     * @param type A byte that tags a serializer
     * @return     A serializer that corresponds to the type byte
     */
    public ISerializer getSerializer(Byte type) {
        if (type <= SYSTEM_SERIALIZERS_COUNT) {
            if (serializersMap.containsKey(type)) {
                return serializersMap.get(type);
            }
        } else if (customSerializers.containsKey(type)) {
            return customSerializers.get(type);
        }

        throw new SerializerException(type);
    }

    /**
     * Register a serializer.
     * @param serializer Serializer to register
     */
    public synchronized void registerSerializer(ISerializer serializer) {
        if (serializer.getType() > SYSTEM_SERIALIZERS_COUNT) {
            customSerializers.put(serializer.getType(), serializer);
        } else {
            String msg = String.format("Serializer id must be greater than %s", SYSTEM_SERIALIZERS_COUNT);
            throw new RuntimeException(msg);
        }
        // clear MVOCache
    }

    /**
     * Clear custom serializers.
     */
    public synchronized void clearCustomSerializers() {
        customSerializers.clear();
    }

    @VisibleForTesting
    public synchronized void removeSerializer(ISerializer serializer) {
        customSerializers.remove(serializer.getType());
    }
}

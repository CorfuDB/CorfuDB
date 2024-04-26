package org.corfudb.util.serializer;

import com.google.common.annotations.VisibleForTesting;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ClassUtils;
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
    public static final CorfuSerializer CORFU = new CorfuSerializer();
    public static final JavaSerializer JAVA = new JavaSerializer();
    public static final JsonSerializer JSON = new JsonSerializer();
    public static final PrimitiveSerializer PRIMITIVE = new PrimitiveSerializer();
    public static final CorfuQueueSerializer QUEUE_SERIALIZER = new CorfuQueueSerializer();

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

    public ProtobufSerializer getProtobufSerializer() {
        ProtobufSerializer protoSerializer;
        try {
            // If protobuf serializer is already registered, reference static/global class map so schemas
            // are shared across all runtime's and not overwritten (if multiple runtime's exist).
            // This aims to overcome a current design limitation where the serializers are static and not
            // per runtime (to be changed).
            protoSerializer = ClassUtils.cast(getSerializer(SerializerType.PROTOBUF.toByte()));
        } catch (SerializerException se) {
            // This means the protobuf serializer had not been registered yet.
            protoSerializer = new ProtobufSerializer();
            registerSerializer(protoSerializer);
        }

        return protoSerializer;
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

    @AllArgsConstructor
    public enum SerializerType {
        CORFU((byte)0),
        JAVA((byte) 1),
        JSON((byte) 2),
        PRIMITIVE((byte) 3),
        QUEUE((byte) 4),
        PROTOBUF((byte) 25),
        CHECKPOINT((byte) 20);

        private final byte serializerType;

        public byte toByte() {
            return serializerType;
        }
    }
}

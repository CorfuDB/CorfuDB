package org.corfudb.util.serializer;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.Record;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.exceptions.SerializerException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * The Protobuf serializer is the main component that allows CorfuStore to use Protobufs to
 * convert a language specific (Java here) object into an identifiable byte buffer that
 * can be then converted back to a language specific object.
 * <p>
 * To achieve this, this serializer requires a map of all seen class types or a classMap
 * and Google Protobuf 3's Any.
 * Any type carries with it a typeUrl which helps identify the class uniquely.
 * This typeUrl is then used to index the classMap to retrieve the actual Protobuf message
 * while deserializing.
 */
@Slf4j
public class ProtobufSerializer implements ISerializer {

    private final byte type;

    public static final byte PROTOBUF_SERIALIZER_CODE = (byte) 25;

    @Getter
    private final ConcurrentMap<String, Class<? extends Message>> classMap;

    public ProtobufSerializer(ConcurrentMap<String, Class<? extends Message>> classMap) {
        this.type = PROTOBUF_SERIALIZER_CODE;
        this.classMap = classMap;
    }

    enum MessageType {
        KEY(1),
        VALUE(2);

        static final Map<Integer, MessageType> valToTypeMap = new HashMap<>();

        static {
            for (MessageType type : MessageType.values()) {
                valToTypeMap.put(type.val, type);
            }
        }

        final int val;

        MessageType(int val) {
            this.val = val;
        }

        public static MessageType valueOf(int val) {
            return valToTypeMap.get(val);
        }
    }

    @Override
    public byte getType() {
        return type;
    }

    /**
     * Deserialize an object from a given byte buffer.
     *
     * @param b The bytebuf to deserialize.
     * @return The deserialized object.
     */
    @Override
    public Object deserialize(ByteBuf b, CorfuRuntime rt) {

        try (ByteBufInputStream bbis = new ByteBufInputStream(b)) {
            MessageType type = MessageType.valueOf(bbis.readInt());
            int size = bbis.readInt();
            byte[] data = new byte[size];
            bbis.readFully(data);
            Record record = Record.parseFrom(data);
            Any payload = record.getPayload();
            if (!classMap.containsKey(payload.getTypeUrl())) {
                log.error(String.format(
                        "Deserialization error: Un-Opened or unknown key/value type [%s]",
                        payload.getTypeUrl()));
                log.error("Dumping all known types in map for debugging\n");
                for (String entry: classMap.keySet()) {
                    log.error(String.format("[%s] -> [%s]", entry, classMap.get(entry)));
                }
                throw new SerializerException(String.format(
                        "Value [%s] unknown. Maybe openTable() has wrong type?",
                        payload.getTypeUrl()));
            }
            Message value = payload.unpack(classMap.get(payload.getTypeUrl()));

            if (type.equals(MessageType.KEY)) {
                return value;
            } else {
                Message metadata = null;
                if (record.hasMetadata()) {
                    Any anyMetadata = record.getMetadata();
                    Class<? extends Message> metadataClass = classMap.get(anyMetadata.getTypeUrl());
                    if (metadataClass == null) {
                        log.error(String.format("Deserialization error: Unknown metadata type [%s]",
                                anyMetadata.getTypeUrl()));
                        log.error("Dumping all known types in map for debugging\n");
                        for (String entry: classMap.keySet()) {
                            log.error(String.format("[%s] -> [%s]", entry, classMap.get(entry)));
                        }
                        throw new SerializerException(String.format(
                                "Metadata [%s] unknown. Maybe openTable() has wrong metadata type?",
                                anyMetadata.getTypeUrl()));
                    }
                    metadata = anyMetadata.unpack(metadataClass);
                }
                return new CorfuRecord(value, metadata);
            }
        } catch (IOException ie) {
            log.error("Exception during deserialization!", ie);
            throw new SerializerException(ie);
        }
    }

    /**
     * Serialize an object into a given byte buffer.
     *
     * @param o The object to serialize.
     * @param b The bytebuf to serialize it into.
     */
    @Override
    public void serialize(Object o, ByteBuf b) {

        Record record;
        MessageType type;

        if (o instanceof CorfuRecord) {
            CorfuRecord corfuRecord = (CorfuRecord) o;
            Any message = Any.pack(corfuRecord.getPayload());
            Record.Builder recordBuilder = Record.newBuilder()
                    .setPayload(message);
            if (corfuRecord.getMetadata() != null) {
                Any metadata = Any.pack(corfuRecord.getMetadata());
                recordBuilder.setMetadata(metadata);
            }
            record = recordBuilder.build();
            type = MessageType.VALUE;
        } else {
            Any message = Any.pack(((Message) o));
            record = Record.newBuilder()
                    .setPayload(message)
                    .build();
            type = MessageType.KEY;
        }
        byte[] data = record.toByteArray();

        try (ByteBufOutputStream bbos = new ByteBufOutputStream(b)) {
            bbos.writeInt(type.val);
            bbos.writeInt(data.length);
            bbos.write(data);
        } catch (IOException ie) {
            log.error("Exception during serialization!", ie);
            throw new SerializerException(ie);
        }
    }
}

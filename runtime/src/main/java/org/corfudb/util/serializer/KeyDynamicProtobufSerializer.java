package org.corfudb.util.serializer;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.OpaqueCorfuDynamicRecord;
import org.corfudb.runtime.exceptions.SerializerException;

import java.io.IOException;

/**
 * This Protobuf serializer class is based on the {@link DynamicProtobufSerializer}. The difference
 * is that this KeyDynamicProtobufSerializer uses {@link OpaqueCorfuDynamicRecord} which has
 * ByteString-format message payload. For the use cases where deserializing Any message from ByteString
 * is not required (such as compactor), this serializer has lower memory usage ({@link DynamicMessage} has overhead)
 * and is more efficient.
 */
@Slf4j
public class KeyDynamicProtobufSerializer extends DynamicProtobufSerializer {

    public KeyDynamicProtobufSerializer(CorfuRuntime corfuRuntime) {
        super(corfuRuntime);
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
            ProtobufSerializer.MessageType type = ProtobufSerializer.MessageType.valueOf(bbis.readInt());
            int size = bbis.readInt();
            byte[] data = new byte[size];
            bbis.readFully(data);
            CorfuStoreMetadata.Record corfuRecord = CorfuStoreMetadata.Record.parseFrom(data);
            Any payload = corfuRecord.getPayload();

            String fullMessageName = getFullMessageName(payload);
            if (!messagesFdProtoNameMap.containsKey(fullMessageName)) {
                log.error("messagesFdProtoNameMap doesn't contain the message type {} of payload {}." +
                                "Please check if the related table is properly opened with correct schema.",
                        fullMessageName, payload);
                log.error("messagesFdProtoNameMap keySet is {}", messagesFdProtoNameMap.keySet());
            }

            if (type.equals(ProtobufSerializer.MessageType.KEY)) {
                Descriptors.FileDescriptor valueFileDescriptor
                        = getDescriptor(messagesFdProtoNameMap.get(fullMessageName));
                Descriptors.Descriptor valueDescriptor = valueFileDescriptor.findMessageTypeByName(getMessageName(payload));
                DynamicMessage value = DynamicMessage.parseFrom(valueDescriptor, payload.getValue());
                return new CorfuDynamicKey(payload.getTypeUrl(), value);
            }

            String metadataTypeUrl = null;
            ByteString metaDataByteString = null;
            if (corfuRecord.hasMetadata()) {
                Any anyMetadata = corfuRecord.getMetadata();
                metadataTypeUrl = anyMetadata.getTypeUrl();
                metaDataByteString = anyMetadata.getValue();

                // Check message type to detect any wrong schema / schema corruption
                String fullMetadataMessageName = getFullMessageName(anyMetadata);
                if (!messagesFdProtoNameMap.containsKey(fullMetadataMessageName)) {
                    log.error("messagesFdProtoNameMap doesn't contain the message type {} of metadata {}." +
                                    "Please check if the related table is properly opened with correct schema.",
                            fullMetadataMessageName, anyMetadata);
                    log.error("messagesFdProtoNameMap keySet is {}", messagesFdProtoNameMap.keySet());
                }
            }

            String metadataTypeUrlIntern = metadataTypeUrl != null ? metadataTypeUrl.intern() : null;
            return new OpaqueCorfuDynamicRecord(payload.getTypeUrl().intern(), payload.getValue(),
                    metadataTypeUrlIntern, metaDataByteString);

        } catch (IOException | Descriptors.DescriptorValidationException ie) {
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

        CorfuStoreMetadata.Record corfuRecord;
        ProtobufSerializer.MessageType messageType;

        if (o instanceof OpaqueCorfuDynamicRecord) {
            OpaqueCorfuDynamicRecord opaqueRecord = (OpaqueCorfuDynamicRecord) o;

            Any message = Any.newBuilder()
                    .setTypeUrl(opaqueRecord.getPayloadTypeUrl().intern())
                    .setValue(opaqueRecord.getPayload())
                    .build();
            CorfuStoreMetadata.Record.Builder recordBuilder = CorfuStoreMetadata.Record.newBuilder()
                    .setPayload(message);
            if (opaqueRecord.getMetadata() != null) {
                Any metadata = Any.newBuilder()
                        .setTypeUrl(opaqueRecord.getMetadataTypeUrl().intern())
                        .setValue(opaqueRecord.getMetadata())
                        .build();
                recordBuilder.setMetadata(metadata);
            }
            corfuRecord = recordBuilder.build();
            messageType = ProtobufSerializer.MessageType.VALUE;
        } else {
            CorfuDynamicKey corfuKey = (CorfuDynamicKey) o;
            Any message = Any.newBuilder()
                    .setTypeUrl(corfuKey.getKeyTypeUrl().intern())
                    .setValue(corfuKey.getKey().toByteString())
                    .build();
            corfuRecord = CorfuStoreMetadata.Record.newBuilder()
                    .setPayload(message)
                    .build();
            messageType = ProtobufSerializer.MessageType.KEY;
        }
        byte[] data = corfuRecord.toByteArray();

        try (ByteBufOutputStream bbos = new ByteBufOutputStream(b)) {
            bbos.writeInt(messageType.val);
            bbos.writeInt(data.length);
            bbos.write(data);
        } catch (IOException ie) {
            log.error("Exception during serialization!", ie);
            throw new SerializerException(ie);
        }
    }

}

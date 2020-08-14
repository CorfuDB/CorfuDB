package org.corfudb.util.serializer;

import static com.google.common.base.Preconditions.checkNotNull;


import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuQueue;

public class CorfuQueueSerializer implements ISerializer {

    // Serialization tag for a queue's metadata entry
    private final byte entryRecordIdMarker = 1;

    // Serialization tag for the queue's payload
    private final byte entryPayloadMarker = 2;

    private final byte type;

    public CorfuQueueSerializer(byte type) {
        this.type = type;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte getType() {
        return type;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object deserialize(ByteBuf b, CorfuRuntime rt) {
        byte type = b.readByte();
        switch (type) {
            case entryRecordIdMarker:
                return CorfuQueue.CorfuRecordId.deserialize(b);
            case entryPayloadMarker:
                int len = b.readInt();
                byte[] payload = new byte[len];
                b.readBytes(payload);
                return ByteString.copyFrom(payload);
            default:
                throw new IllegalArgumentException("Unknown type! " + type);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void serialize(Object o, ByteBuf b) {
        checkNotNull(o);
        if (o instanceof CorfuQueue.CorfuRecordId) {
            CorfuQueue.CorfuRecordId recordId = (CorfuQueue.CorfuRecordId) o;
            b.writeByte(entryRecordIdMarker);
            recordId.serialize(b);
            return;
        } else if (o instanceof ByteString) {
            b.writeByte(entryPayloadMarker);
            ByteString payload = (ByteString) o;
            b.writeInt(payload.size());
            b.writeBytes(payload.asReadOnlyByteBuffer());
            return;
        } else {
            throw new IllegalArgumentException("Unknown type! " + o.getClass().getSimpleName());
        }
    }
}

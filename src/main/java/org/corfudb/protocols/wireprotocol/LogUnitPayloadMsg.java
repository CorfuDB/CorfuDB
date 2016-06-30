package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.CorfuSerializer;
import org.corfudb.util.serializer.ISerializer;

/**
 * Created by mwei on 9/17/15.
 */
@ToString
public class LogUnitPayloadMsg extends LogUnitMetadataMsg {

    /** The default serializer to use */
    public static final ISerializer defaultSerializer = new CorfuSerializer();

    /** The object payload, if sending the message. */
    @Setter
    Object payload;

    /** The ByteBuf, if receiving the message. */
    @Setter
    ByteBuf data;

    /** The serializer to use. */
    @Getter
    @Setter
    ISerializer serializer = defaultSerializer;

    public Object getPayload(CorfuRuntime rt)
    {
        Object ret =
                (payload != null) ? payload :
                (data == null) ? null : serializer.deserialize(data, rt);
        if (data != null)
        {
            data.release();
            data = null;
        }
        payload = ret;
        return ret;
    }

    public ByteBuf getData()
    {
        if (data == null)
        {
            ByteBuf d = UnpooledByteBufAllocator.DEFAULT.buffer();
            serializer.serialize(payload, d);
            data = d;
        }
        return data.duplicate();
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        int index = buffer.writerIndex();
        buffer.writeInt(0);
        if (payload != null) {
            serializer.serialize(payload, buffer);
            int finalIndex = buffer.writerIndex();
            //this is the total size written by the serializer
            buffer.setInt(index, finalIndex - index - 4);
        }
        else if (data != null)
        {
            ByteBuf o = data.duplicate();
            buffer.writeBytes(o);
            buffer.setInt(index, o.readerIndex());
        }
    }

    /**
     * Parse the rest of the message from the buffer. Classes that extend CorfuMsg
     * should parse their fields in this method.
     *
     * @param buffer
     */
    @Override
    public void fromBuffer(ByteBuf buffer) {
        super.fromBuffer(buffer);
        int length = buffer.readInt();
        data = length == 0 ? null : buffer.slice(buffer.readerIndex(), length);
        if (data != null) {buffer.retain();}
        buffer.skipBytes(length);
    }
}

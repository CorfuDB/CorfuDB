package org.corfudb.infrastructure.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.KryoSerializer;

/**
 * Created by mwei on 9/17/15.
 */

public class NettyLogUnitPayloadMsg extends NettyLogUnitMetadataMsg {

    /** The default serializer to use */
    public static final ISerializer defaultSerializer = new KryoSerializer();

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

    public Object getPayload()
    {
        Object ret =
                (payload != null) ? payload :
                (data == null) ? null : serializer.deserialize(data);
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
     * Parse the rest of the message from the buffer. Classes that extend NettyCorfuMsg
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

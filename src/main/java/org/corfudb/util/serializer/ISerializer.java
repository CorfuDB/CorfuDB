package org.corfudb.util.serializer;

import io.netty.buffer.ByteBuf;

/**
 * This class represents a serializer, which takes an object and reads/writes it to a bytebuf.
 * Created by mwei on 9/17/15.
 */
public interface ISerializer {

    /** Deserialize an object from a given byte buffer.
     *
     * @param b The bytebuf to deserialize.
     * @return  The deserialized object.
     */
    Object deserialize(ByteBuf b);

    /** Serialize an object into a given byte buffer.
     *
     * @param o The object to serialize.
     * @param b The bytebuf to serialize it into.
     */
    void serialize(Object o, ByteBuf b);
}

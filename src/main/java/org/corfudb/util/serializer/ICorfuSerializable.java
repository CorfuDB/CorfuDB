package org.corfudb.util.serializer;

import io.netty.buffer.ByteBuf;

/**
 * Created by mwei on 9/29/15.
 */
public interface ICorfuSerializable {
    void serialize(ByteBuf b);
}

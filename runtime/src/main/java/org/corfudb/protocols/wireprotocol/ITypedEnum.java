package org.corfudb.protocols.wireprotocol;

import com.google.common.reflect.TypeToken;
import io.netty.buffer.ByteBuf;

/**
 * Created by mwei on 8/9/16.
 */
public interface ITypedEnum<T extends Enum<T>>  {

    TypeToken<?> getComponentType();

    byte asByte();

    default void doSerialize(ByteBuf buf) {
        buf.writeByte(this.asByte());
    }
}

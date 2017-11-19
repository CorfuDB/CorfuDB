package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.Range;

import io.netty.buffer.ByteBuf;

import java.util.Collections;
import java.util.UUID;

import java.util.concurrent.atomic.LongAdder;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by mwei on 8/11/16.
 */
@Data
public class ReadRequest implements ICorfuPayload<ReadRequest> {

    final byte type;
    static final byte READ_SINGLE = 0;
    static final byte READ_MULTIPLE = 1;

    final Iterable<Long> addresses;
    /**
     * Deserialization Constructor from ByteBuf to ReadRequest.
     *
     * @param buf The buffer to deserialize
     */
    public ReadRequest(ByteBuf buf) {
        type = buf.readByte();
        if (type == READ_SINGLE) {
            addresses = Collections.singletonList(buf.readLong());
        } else if (type == READ_MULTIPLE) {
            addresses = ICorfuPayload.listFromBuffer(buf, Long.class);
        } else {
            throw new UnsupportedOperationException("Unknown read request type");
        }
    }

    public ReadRequest(Iterable<Long> addresses) {
        type = READ_MULTIPLE;
        this.addresses = addresses;
    }

    public ReadRequest(Long address) {
        type = READ_SINGLE;
        addresses = Collections.singletonList(address);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeByte(type);
        if (type == READ_SINGLE) {
            buf.writeLong(addresses.iterator().next());
        } else if (type == READ_MULTIPLE) {
            LongAdder adder = new LongAdder();
            int sizeIndex = buf.writerIndex();
            buf.writeInt(0);
            addresses.iterator()
                    .forEachRemaining(a -> {
                        adder.add(1);
                        buf.writeLong(a);
                    });
            int end = buf.writerIndex();
            buf.writerIndex(sizeIndex);
            buf.writeInt(adder.intValue());
            buf.writerIndex(end);
        } else {
            throw new UnsupportedOperationException();
        }
    }

}

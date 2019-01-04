package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import org.corfudb.runtime.view.Layout;

/**
 * Created by Maithem on 1/3/19.
 */

public class LayoutQueryRequest implements ICorfuPayload<LayoutQueryRequest> {

    @Getter
    final Type queryType;

    @Getter
    final long epoch;

    public LayoutQueryRequest(Type type, long epoch) {
        queryType = type;

        if (queryType == Type.COMMITTED) {
            this.epoch = epoch;
        } else {
            this.epoch = Layout.INVALID_EPOCH;
        }
    }

    public LayoutQueryRequest(ByteBuf buf) {
        queryType = Type.values()[buf.readInt()];

        if (queryType == Type.COMMITTED) {
            this.epoch = buf.readLong();
        } else {
            this.epoch = Layout.INVALID_EPOCH;
        }
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeInt(queryType.getValue());
        if (queryType == Type.COMMITTED) {
            buf.writeLong(epoch);
        }
    }

    public enum Type {
        COMMITTED(0),
        PHASE2(1);

        private final int value;

        Type(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }
}

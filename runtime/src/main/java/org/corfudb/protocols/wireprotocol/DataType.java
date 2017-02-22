package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by mwei on 8/16/16.
 */
@RequiredArgsConstructor
public enum DataType implements ICorfuPayload<DataType> {
    DATA(0) {
        @Override
        public boolean isHole() {
            return false;
        }

        @Override
        public boolean isData() {
            return true;
        }

        @Override
        public boolean isProposal() {
            return false;
        }
    },
    EMPTY(1) {
        @Override
        public boolean isHole() {
            return false;
        }

        @Override
        public boolean isData() {
            return false;
        }

        @Override
        public boolean isProposal() {
            return false;
        }
    },
    HOLE(2) {
        @Override
        public boolean isHole() {
            return true;
        }

        @Override
        public boolean isData() {
            return false;
        }

        @Override
        public boolean isProposal() {
            return false;
        }
    },
    TRIMMED(3) {
        @Override
        public boolean isHole() {
            return false;
        }

        @Override
        public boolean isData() {
            return false;
        }

        @Override
        public boolean isProposal() {
            return false;
        }
    },
    DATA_PROPOSED(4) {
        @Override
        public boolean isHole() {
            return false;
        }

        @Override
        public boolean isData() {
            return true;
        }

        @Override
        public boolean isProposal() {
            return true;
        }
    },
    HOLE_PROPOSED(5) {
        @Override
        public boolean isHole() {
            return true;
        }

        @Override
        public boolean isData() {
            return false;
        }

        @Override
        public boolean isProposal() {
            return true;
        }
    };

    final int val;

    public abstract boolean isHole();
    public abstract boolean isData();
    public abstract boolean isProposal();


    byte asByte() {
        return (byte) val;
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeByte(asByte());
    }

    public static Map<Byte, DataType> typeMap =
            Arrays.stream(DataType.values())
                    .collect(Collectors.toMap(DataType::asByte, Function.identity()));

}

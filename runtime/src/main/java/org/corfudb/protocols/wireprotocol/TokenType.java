package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by dalia on 4/8/17.
 */
@RequiredArgsConstructor
public enum  TokenType implements ICorfuPayload<TokenType> {
    NORMAL((byte) 0),
    QUERY((byte)1),
    TX_ABORT_CONFLICT((byte)2),
    TX_ABORT_NEWSEQ((byte) 3);

    final int val;

    byte asByte() {
        return (byte) val;
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeByte(asByte());
    }

    static Map<Byte, TokenType> typeMap =
            Arrays.stream(TokenType.values())
                    .collect(Collectors.toMap(TokenType::asByte, Function.identity()));

}

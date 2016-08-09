package org.corfudb.infrastructure.log;

import io.netty.buffer.ByteBuf;
import lombok.RequiredArgsConstructor;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;

/**
 * Created by mwei on 8/8/16.
 */
public class LogAddress implements ICorfuPayload<LogAddress> {



    @Override
    public void doSerialize(ByteBuf buf) {

    }
}

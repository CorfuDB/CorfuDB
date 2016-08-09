package org.corfudb.infrastructure.log;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;

import java.util.UUID;

/**
 * Created by mwei on 8/8/16.
 */
@Data
@RequiredArgsConstructor
public class LogAddress {
    final Long address;
    final UUID stream;
}

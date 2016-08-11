package org.corfudb.infrastructure.log;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.corfudb.protocols.wireprotocol.IMetadata;

import java.util.EnumMap;

@Data
@RequiredArgsConstructor
@AllArgsConstructor
public class LogUnitEntry implements IMetadata {
    public final long address;
    public final ByteBuf buffer;
    public final EnumMap<LogUnitMetadataType, Object> metadataMap;
    public final boolean isHole;
    public boolean isPersisted;

    /**
     * Generate a new log unit entry which is a hole
     */
    public LogUnitEntry(long address) {
        this.address = address;
        buffer = null;
        this.metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);
        this.isHole = true;
        this.isPersisted = false;
    }
}

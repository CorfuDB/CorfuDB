package org.corfudb.runtime.collections;

import lombok.Getter;
import javax.annotation.Nullable;

public class FullStateMessage {
    @Getter
    private final DataLocator dataLocator;

    @Getter
    private final byte[] payload;

    @Getter
    private final byte[] metaValue;

    public FullStateMessage(DataLocator dataLocator, byte[] payload, @Nullable byte[] metaValue) {
        this.dataLocator = dataLocator;
        this.payload = payload;
        this.metaValue = metaValue;
    }
}

package org.corfudb.runtime.exceptions;

import lombok.Getter;
import java.util.UUID;

public class StaleObjectVersionException extends RuntimeException {

    @Getter
    private final UUID objectId;

    @Getter
    private final long timestamp;

    public StaleObjectVersionException(UUID objectId, long timestamp) {
        super("Stale object version. [object ID=" + objectId +
                ", timestamp=" + timestamp + "]");
        this.objectId = objectId;
        this.timestamp = timestamp;
    }

}

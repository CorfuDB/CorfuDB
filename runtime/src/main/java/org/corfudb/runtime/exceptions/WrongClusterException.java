package org.corfudb.runtime.exceptions;

import java.util.UUID;
import javax.annotation.Nullable;
import lombok.Getter;

public class WrongClusterException extends RuntimeException {

    /** The cluster we expected to connect to. */
    @Getter
    final UUID expectedCluster;

    /** The cluster we actually ended up connecting to. */
    @Getter
    final UUID actualCluster;

    /** Create a new {@link org.corfudb.runtime.exceptions.WrongClusterException}.
     *
     * @param expectedCluster   The cluster we expected to connect to.
     * @param actualCluster     The cluster we actually ended up connecting to.
     */
    public WrongClusterException(@Nullable UUID expectedCluster,
                                 @Nullable UUID actualCluster) {
        super("Expected: "
            + ((expectedCluster == null) ? "null" : expectedCluster)
            + " Actual: "
            + ((actualCluster == null) ? "null" : actualCluster));
        this.expectedCluster = expectedCluster;
        this.actualCluster = actualCluster;
    }
}

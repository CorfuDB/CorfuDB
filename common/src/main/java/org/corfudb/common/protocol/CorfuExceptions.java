package org.corfudb.common.protocol;

import lombok.Getter;
import org.corfudb.common.util.UuidUtils;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * Created by Maithem on 7/1/20.
 */

public class CorfuExceptions extends Exception {

    public CorfuExceptions(String msg) {
        super(msg);
    }

    public CorfuExceptions() {
        super();
    }

    public static class PeerUnavailable extends CorfuExceptions {

        final InetSocketAddress address;

        public PeerUnavailable(InetSocketAddress address) {
            super();
            this.address = address;
        }
    }

    public static class WrongEpochException extends CorfuExceptions {
        @Getter
        final long correctEpoch;
        public WrongEpochException(long correctEpoch) {
            super("Wrong epoch. [expected=" + correctEpoch + "]");
            this.correctEpoch = correctEpoch;
        }
    }

    public class WrongClusterException extends CorfuExceptions {

        /** The cluster we expected to connect to. */
        @Getter
        final UUID expectedCluster;

        /** The cluster we actually ended up connecting to. */
        @Getter
        final UUID actualCluster;

        /** Create a new {@link org.corfudb.common.protocol.CorfuExceptions.WrongClusterException}.
         *
         * @param expectedCluster   The cluster we expected to connect to.
         * @param actualCluster     The cluster we actually ended up connecting to.
         */
        public WrongClusterException(@Nullable UUID expectedCluster,
                                     @Nullable UUID actualCluster) {
            super("Expected: "
                    + ((expectedCluster == null) ? "null" : UuidUtils.asBase64(expectedCluster))
                    + " Actual: "
                    + ((actualCluster == null) ? "null" : UuidUtils.asBase64(actualCluster)));
            this.expectedCluster = expectedCluster;
            this.actualCluster = actualCluster;
        }
    }
}
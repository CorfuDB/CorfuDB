package org.corfudb.common.protocol;

import java.net.InetSocketAddress;

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
}
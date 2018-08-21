package org.corfudb.universe.node;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.slf4j.event.Level;

/**
 * Represent a Corfu server implementation of {@link Node} used in the {@link org.corfudb.universe.cluster.Cluster}.
 */
public interface CorfuServer extends Node {

    @Override
    CorfuServer deploy() throws NodeException;

    enum Mode {
        SINGLE, CLUSTER
    }

    enum Persistence {
        DISK, MEMORY
    }

    ServerParams getParams();

    @Builder
    @Getter
    @AllArgsConstructor
    class ServerParams implements NodeParams {

        private final String logDir;
        private final int port;
        private final Mode mode;
        private final Persistence persistence;
        private final Level logLevel;
        private final String host;

        public String getGenericName() {
            return "node" + port;
        }

        public String getEndpoint(){
            return getGenericName() + ":" + port;
        }
    }
}

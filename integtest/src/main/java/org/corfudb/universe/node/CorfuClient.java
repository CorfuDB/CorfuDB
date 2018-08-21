package org.corfudb.universe.node;

import lombok.Builder;
import lombok.Getter;

/**
 * Represent a Corfu client implementation of {@link Node}.
 */
public interface CorfuClient extends Node {
    ClientParams getParams();

    @Builder
    @Getter
    class ClientParams implements NodeParams {
        private final String host;
        public String getHost() {
            return this.host;
        }
    }
}

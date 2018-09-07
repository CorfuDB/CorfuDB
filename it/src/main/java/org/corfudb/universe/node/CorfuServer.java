package org.corfudb.universe.node;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.runtime.view.Layout;
import org.slf4j.event.Level;

import java.time.Duration;
import java.util.Optional;

import static lombok.EqualsAndHashCode.Exclude;

/**
 * Represent a Corfu server implementation of {@link Node} used in the {@link org.corfudb.universe.cluster.Cluster}.
 */
public interface CorfuServer extends Node {

    @Override
    CorfuServer deploy();

    boolean addNode(CorfuServer server);

    boolean removeNode(CorfuServer server);

    ServerParams getParams();

    Optional<Layout> getLayout();

    void connectCorfuRuntime();

    enum Mode {
        SINGLE, CLUSTER
    }

    enum Persistence {
        DISK, MEMORY
    }

    @Builder
    @Getter
    @AllArgsConstructor
    @EqualsAndHashCode
    @ToString
    class ServerParams implements NodeParams {
        @Exclude
        private final String logDir;
        private final int port;
        private final Mode mode;
        private final Persistence persistence;
        @Exclude
        private final Level logLevel;

        @Exclude
        private final int workflowNumRetry;
        @Exclude
        private final Duration timeout;
        @Exclude
        private final Duration pollPeriod;

        public String getGenericName() {
            return "node" + port;
        }

        public String getEndpoint() {
            return getGenericName() + ":" + port;
        }
    }
}

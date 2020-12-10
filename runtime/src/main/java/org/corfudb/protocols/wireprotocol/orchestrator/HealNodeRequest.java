package org.corfudb.protocols.wireprotocol.orchestrator;

import lombok.Getter;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.HEAL_NODE;

/**
 * An orchestrator request to heal an existing node to the cluster.
 *
 * @author Maithem
 */
public class HealNodeRequest implements Request {

    @Getter
    public String endpoint;

    @Getter
    boolean layoutServer;

    @Getter
    boolean sequencerServer;

    @Getter
    boolean logUnitServer;

    @Getter
    int stripeIndex;

    public HealNodeRequest(String endpoint,
                           boolean layoutServer,
                           boolean sequencerServer,
                           boolean logUnitServer,
                           int stripeIndex) {
        this.endpoint = endpoint;
        this.layoutServer = layoutServer;
        this.sequencerServer = sequencerServer;
        this.logUnitServer = logUnitServer;
        this.stripeIndex = stripeIndex;
    }

    @Override
    public OrchestratorRequestType getType() {
        return HEAL_NODE;
    }
}

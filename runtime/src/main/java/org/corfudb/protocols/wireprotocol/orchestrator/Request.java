package org.corfudb.protocols.wireprotocol.orchestrator;

/**
 *
 * An interface that should be implemented by all the orchestrator service requests.
 *
 * @author Maithem
 */
public interface Request {

    /**
     * Returns the type of the request.
     *
     * @return type of request
     */
    OrchestratorRequestType getType();

    /**
     * The endpoint of the node to operate on (i.e. endpoint of a new node to be added)
     * @return the endpoint
     */
    String getEndpoint();
}

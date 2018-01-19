package org.corfudb.protocols.wireprotocol.orchestrator;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The type of requests that can be made to the Orchestrator Service.
 *
 * @author Maithem
 */
@AllArgsConstructor
public enum OrchestratorRequestType {

    /**
     * Query a workflow id
     */
    QUERY(0, QueryRequest::new),

    /**
     * Add a new node to the cluster
     */
    ADD_NODE(1, AddNodeRequest::new),

    /**
     * Remove a node from the cluster
     */
    REMOVE_NODE(2, RemoveNodeRequest::new),

    /**
     * Heal an existing node in the cluster
     */
    HEAL_NODE(3, HealNodeRequest::new),

    /**
     * Force remove a node from the cluster
     */
    FORCE_REMOVE_NODE(4, ForceRemoveNodeRequest::new);


    @Getter
    public final int type;

    @Getter
    final Function<byte[], Request> requestGenerator;

    /**
     * Map an int to an enum.
     */
    static final Map<Integer, OrchestratorRequestType> typeMap =
            Arrays.stream(OrchestratorRequestType.values())
                    .collect(Collectors.toMap(OrchestratorRequestType::getType, Function.identity()));
}

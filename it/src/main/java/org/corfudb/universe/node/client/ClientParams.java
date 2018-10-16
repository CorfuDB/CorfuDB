package org.corfudb.universe.node.client;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.Node.NodeParams;
import org.corfudb.universe.node.Node.NodeType;

import java.time.Duration;

@Builder
@Getter
@EqualsAndHashCode(exclude = {"numRetry", "timeout", "pollPeriod"})
public
class ClientParams implements NodeParams {
    @Default
    @NonNull
    private final String name = "corfuClient";
    @NonNull
    private final NodeType nodeType = NodeType.CORFU_CLIENT;
    /**
     * Total number of times to retry a workflow if it fails
     */
    @Default
    @NonNull
    private final int numRetry = 5;
    /**
     * Total time to wait before the workflow times out
     */
    @Default
    @NonNull
    private final Duration timeout = Duration.ofSeconds(30);
    /**
     * Poll period to query the completion of the workflow
     */
    @Default
    @NonNull
    private final Duration pollPeriod = Duration.ofMillis(50);
}

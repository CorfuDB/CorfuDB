package org.corfudb.universe.node.server;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.universe.node.Node.NodeParams;
import org.corfudb.universe.node.Node.NodeType;
import org.corfudb.universe.node.server.CorfuServer.Mode;
import org.corfudb.universe.node.server.CorfuServer.Persistence;
import org.slf4j.event.Level;

import java.time.Duration;

@Builder(builderMethodName = "serverParamsBuilder")
@Getter
@AllArgsConstructor
@EqualsAndHashCode(exclude = {"streamLogDir", "logLevel", "stopTimeout"})
@ToString
public
class CorfuServerParams implements NodeParams {
    @Default
    @NonNull
    private final String streamLogDir = "/tmp/";
    @Default
    @NonNull
    private final int port = 9000;
    @Default
    @NonNull
    private final Mode mode = Mode.CLUSTER;
    @Default
    @NonNull
    private final Persistence persistence = Persistence.DISK;
    @Default
    @NonNull
    private final Level logLevel = Level.DEBUG;
    @NonNull
    private final NodeType nodeType = NodeType.CORFU_SERVER;
    /**
     * A name of the Corfu cluster
     */
    @Getter
    @NonNull
    private final String clusterName;
    @Getter
    @Default
    @NonNull
    private final Duration stopTimeout = Duration.ofSeconds(1);

    @Override
    public String getName() {
        return clusterName + "-corfu-node" + port;
    }
}

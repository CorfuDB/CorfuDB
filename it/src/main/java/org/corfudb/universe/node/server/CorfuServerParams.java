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
@AllArgsConstructor
@EqualsAndHashCode(exclude = {"logLevel", "stopTimeout"})
@ToString
public
class CorfuServerParams implements NodeParams {
    @NonNull
    private final String streamLogDir = "db";
    @Default
    @Getter
    private final int port = 9000;
    @Default
    @NonNull
    @Getter
    private final Mode mode = Mode.CLUSTER;
    @Default
    @NonNull
    @Getter
    private final Persistence persistence = Persistence.DISK;
    @Default
    @NonNull
    @Getter
    private final Level logLevel = Level.DEBUG;
    @NonNull
    @Getter
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

    public String getStreamLogDir(){
        return getName() + "/" + streamLogDir;
    }
}

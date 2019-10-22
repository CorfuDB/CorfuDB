package org.corfudb.universe.node.server;

import com.google.common.collect.ImmutableSet;
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
import java.util.Set;

@Builder(builderMethodName = "serverParamsBuilder")
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class CorfuServerParams implements NodeParams {
    @NonNull
    private final String streamLogDir = "db";

    @Default
    @Getter
    private final int port = ServerUtil.getRandomOpenPort();

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
    @EqualsAndHashCode.Exclude
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
    @EqualsAndHashCode.Exclude
    private final Duration stopTimeout = Duration.ofSeconds(1);

    @Override
    public String getName() {
        return clusterName + "-corfu-node" + getPort();
    }

    public String getStreamLogDir() {
        return getName() + "/" + streamLogDir;
    }

    @Override
    public Set<Integer> getPorts() {
        return ImmutableSet.of(port);
    }
}

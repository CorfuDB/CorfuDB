package org.corfudb.universe.group.cluster;

import com.google.common.collect.ImmutableSortedSet;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.apache.commons.lang3.RandomStringUtils;
import org.corfudb.universe.group.Group.GroupParams;
import org.corfudb.universe.group.cluster.Cluster.ClusterType;
import org.corfudb.universe.node.Node.NodeType;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.common.util.ClassUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

@Builder
@EqualsAndHashCode
@ToString
public class CorfuClusterParams implements GroupParams<CorfuServerParams> {

    @Getter
    @Default
    @NonNull
    private String name = RandomStringUtils.randomAlphabetic(6).toLowerCase();

    @Default
    @Getter
    private final int numNodes = 3;

    /**
     * Corfu server version, for instance: 0.3.0-SNAPSHOT
     */
    @NonNull
    @Getter
    private final String serverVersion;

    @Default
    @NonNull
    private final SortedSet<CorfuServerParams> nodes = new TreeSet<>();

    @Getter
    @Default
    @NonNull
    private NodeType nodeType = NodeType.CORFU_SERVER;

    @Default
    @Getter
    @NonNull
    private final int bootStrapRetries = 20;

    @Default
    @Getter
    @NonNull
    private final Duration retryDuration = Duration.ofSeconds(3);

    @Override
    public ClusterType getType() {
        return ClusterType.CORFU_CLUSTER;
    }

    @Override
    public ImmutableSortedSet<CorfuServerParams> getNodesParams() {
        return ImmutableSortedSet.copyOf(nodes);
    }

    public synchronized CorfuServerParams getNode(String serverName) {
        Map<String, CorfuServerParams> nodesMap = nodes
                .stream()
                .collect(Collectors.toMap(CorfuServerParams::getName, n -> n));

        return nodesMap.get(serverName);
    }

    @Override
    public synchronized CorfuClusterParams add(CorfuServerParams nodeParams) {
        nodes.add(ClassUtils.cast(nodeParams));
        return this;
    }

    public String getFullNodeName(String nodeName) {
        return name + "-corfu-" + nodeName;
    }

    public int size(){
        return getNodesParams().size();
    }

    public List<String> getClusterNodes() {
        return getNodesParams().stream()
                .map(CorfuServerParams::getName)
                .collect(Collectors.toList());
    }
}

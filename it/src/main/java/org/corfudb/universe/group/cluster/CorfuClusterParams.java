package org.corfudb.universe.group.cluster;

import com.google.common.collect.ImmutableSortedSet;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.apache.commons.lang3.RandomStringUtils;
import org.corfudb.common.util.ClassUtils;
import org.corfudb.universe.group.Group.GroupParams;
import org.corfudb.universe.group.cluster.Cluster.ClusterType;
import org.corfudb.universe.node.Node.NodeType;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.node.server.CorfuServerParams.ContainerResources;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

@Builder
@EqualsAndHashCode
@ToString
public class CorfuClusterParams<T extends CorfuServerParams> implements GroupParams<T> {

    @Getter
    @Default
    @NonNull
    private String name = RandomStringUtils.randomAlphabetic(6).toLowerCase();

    @Default
    @Getter
    private final int numNodes = 3;

    @Default
    @Getter
    private final ContainerResources containerResources =
            ContainerResources.builder().build();

    /**
     * Corfu server version, for instance: 0.0.0.0-SNAPSHOT
     */
    @NonNull
    @Getter
    private final String serverVersion;

    @Default
    @NonNull
    private final SortedSet<T> nodes = new TreeSet<>();

    @Getter
    @Default
    @NonNull
    private final NodeType nodeType = NodeType.CORFU_SERVER;

    @Default
    @Getter
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
    public ImmutableSortedSet<T> getNodesParams() {
        return ImmutableSortedSet.copyOf(nodes);
    }

    public synchronized T getNode(String serverName) {
        Map<String, T> nodesMap = nodes
                .stream()
                .collect(Collectors.toMap(CorfuServerParams::getName, n -> n));

        return nodesMap.get(serverName);
    }

    @Override
    public synchronized CorfuClusterParams<T> add(T nodeParams) {
        nodes.add(ClassUtils.cast(nodeParams));
        return this;
    }

    public String getFullNodeName(String nodeName) {
        return name + "-corfu-" + nodeName;
    }

    public int size() {
        return getNodesParams().size();
    }

    public List<String> getClusterNodes() {
        return getNodesParams().stream()
                .map(CorfuServerParams::getName)
                .collect(Collectors.toList());
    }
}

package org.corfudb.universe.group.cluster;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.apache.commons.lang3.RandomStringUtils;
import org.corfudb.universe.group.Group.GroupParams;
import org.corfudb.universe.node.Node.NodeType;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.util.ClassUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Builder
@EqualsAndHashCode
@ToString
public
class CorfuClusterParams implements GroupParams {
    @Getter
    @Default
    @NonNull
    private String name = RandomStringUtils.randomAlphabetic(6).toLowerCase();
    @Default
    @NonNull
    private final List<CorfuServerParams> nodes = new ArrayList<>();
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
    private final Duration retryTimeout = Duration.ofSeconds(3);

    @Override
    public ImmutableList<CorfuServerParams> getNodesParams() {
        return ImmutableList.copyOf(nodes);
    }

    public synchronized CorfuServerParams getNode(String serverName) {
        Map<String, CorfuServerParams> nodesMap = nodes
                .stream()
                .collect(Collectors.toMap(CorfuServerParams::getName, n -> n));

        return nodesMap.get(serverName);
    }

    public synchronized CorfuClusterParams add(CorfuServerParams nodeParams) {
        nodes.add(ClassUtils.cast(nodeParams));
        return this;
    }

    public String getFullNodeName(String nodeName) {
        return name + "-corfu-" + nodeName;
    }
}

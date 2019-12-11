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
import org.corfudb.universe.node.server.SupportServerParams;

import java.util.SortedSet;
import java.util.TreeSet;

@Builder
@EqualsAndHashCode
@ToString
public class SupportClusterParams implements GroupParams<SupportServerParams> {

    @Getter
    @Default
    @NonNull
    private final String name = RandomStringUtils.randomAlphabetic(6).toLowerCase();

    @Default
    @NonNull
    private final SortedSet<SupportServerParams> nodes = new TreeSet<>();

    @Override
    public ClusterType getType() {
        return ClusterType.SUPPORT_CLUSTER;
    }

    @Override
    public ImmutableSortedSet<SupportServerParams> getNodesParams() {
        return ImmutableSortedSet.copyOf(nodes);
    }

    @Override
    public GroupParams<SupportServerParams> add(SupportServerParams nodeParams) {
        nodes.add(nodeParams);
        return this;
    }

    public String getFullNodeName(String nodeName) {
        return name + "-support-" + nodeName;
    }

}

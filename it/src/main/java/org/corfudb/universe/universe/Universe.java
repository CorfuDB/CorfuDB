package org.corfudb.universe.universe;

import com.google.common.collect.ImmutableMap;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.corfudb.universe.group.Group;
import org.corfudb.universe.group.Group.GroupParams;
import org.corfudb.universe.node.Node;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static lombok.Builder.Default;
import static lombok.EqualsAndHashCode.Exclude;

/**
 * A Universe represents a common notion of a universe of nodes. The architecture of a universe is composed of collections
 * of {@link Group}s and {@link Node}s.
 * Each instance of service in the universe is composed of a collection of {@link Node}s which are a subset of the
 * entire nodes included in the universe.
 * {@link Universe} configuration is provided by an instance of {@link UniverseParams}.
 * <p>
 * The following are the main functionalities provided by this class:
 * DEPLOY: create a {@link Universe} according to the {@link Universe} parameters.
 * SHUTDOWN: shutdown a {@link Universe}.
 * Depending on the underlying deployment this might translate into stopping the {@link Node}-s and/or shutting down the network.
 */
public interface Universe {

    /**
     * Create a {@link Universe} according to the desired state mentioned by {@link UniverseParams}
     *
     * @return an instance of applied change in the {@link Universe}
     * @throws UniverseException
     */
    Universe deploy();

    /**
     * Shutdown the entire {@link Universe} by shutting down all the {@link Group}s in {@link Universe}
     *
     * @throws UniverseException
     */
    void shutdown();

    Universe add(GroupParams groupParams);

    /**
     * Returns an instance of {@link UniverseParams} representing the configuration for the {@link Universe}.
     *
     * @return an instance of {@link UniverseParams}
     */
    UniverseParams getUniverseParams();

    /**
     * Returns an {@link ImmutableMap} of {@link Group}s contained in the {@link Universe}.
     *
     * @return {@link Group}s in the {@link Universe}
     */
    ImmutableMap<String, Group> groups();

    <T extends Group> T getGroup(String groupName);

    @Builder(toBuilder = true)
    @EqualsAndHashCode
    class UniverseParams {
        private static final int TIMEOUT_IN_SECONDS = 5;
        private static final String NETWORK_PREFIX = "CorfuNet";

        @Getter
        @Default
        private final String networkName = NETWORK_PREFIX + UUID.randomUUID().toString();
        @Default
        private final ConcurrentMap<String, GroupParams> groups = new ConcurrentHashMap<>();

        /**
         * {@link Universe} timeout to wait until an action is completed.
         * For instance: stop a service, stop a node, shutdown {@link Universe}
         */
        @Getter
        @Exclude
        @Default
        private final Duration timeout = Duration.ofSeconds(TIMEOUT_IN_SECONDS);

        /**
         * Returns the configuration of a particular service by the name
         *
         * @param name group name
         * @return an instance of {@link GroupParams} representing particular type of a group
         */
        public <T extends GroupParams> T getGroupParams(String name, Class<T> groupType) {
            return groupType.cast(groups.get(name));
        }

        public UniverseParams add(GroupParams groupParams){
            groups.put(groupParams.getName(), groupParams);
            return this;
        }

        public ImmutableMap<String, GroupParams> getGroups() {
            return ImmutableMap.copyOf(groups);
        }
    }
}
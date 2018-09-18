package org.corfudb.universe.scenario.fixture;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Getter;
import org.corfudb.universe.group.CorfuCluster.CorfuClusterParams;
import org.corfudb.universe.group.Group.GroupParams;
import org.corfudb.universe.node.Node;
import org.slf4j.event.Level;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static lombok.Builder.Default;
import static org.corfudb.universe.node.CorfuServer.Mode;
import static org.corfudb.universe.node.CorfuServer.Persistence;
import static org.corfudb.universe.node.CorfuServer.ServerParams;
import static org.corfudb.universe.universe.Universe.UniverseParams;

/**
 * Fixture factory provides predefined fixtures
 */
public interface Fixtures {

    @Builder
    @Getter
    class SingleServerFixture implements Fixture<ServerParams> {
        @Default
        private final int port = 9000;
        @Default
        private final Mode mode = Mode.SINGLE;

        @Override
        public ServerParams data() {
            return ServerParams.builder()
                    .mode(mode)
                    .logDir("/tmp/")
                    .logLevel(Level.TRACE)
                    .persistence(Persistence.MEMORY)
                    .port(port)
                    .timeout(Duration.ofMinutes(5))
                    .pollPeriod(Duration.ofMillis(50))
                    .workflowNumRetry(3)
                    .build();
        }
    }

    @Builder
    @Getter
    class MultipleServersFixture implements Fixture<ImmutableList<ServerParams>> {
        @Default
        private final int numNodes = 3;

        @Override
        public ImmutableList<ServerParams> data() {
            List<ServerParams> serversParams = new ArrayList<>();

            for (int i = 0; i < numNodes; i++) {
                final int port = 9000 + i;
                Mode mode = Mode.CLUSTER;

                ServerParams serverParam = SingleServerFixture
                        .builder()
                        .port(port)
                        .mode(mode)
                        .build()
                        .data();

                serversParams.add(serverParam);
            }
            return ImmutableList.copyOf(serversParams);
        }
    }

    @Builder
    @Getter
    class CorfuGroupFixture implements Fixture<GroupParams> {
        @Default
        private final MultipleServersFixture servers = MultipleServersFixture.builder().build();
        @Default
        private final String groupName = "corfuServer";

        @Override
        public GroupParams data() {
            CorfuClusterParams params = CorfuClusterParams.builder()
                    .name(groupName)
                    .nodeType(Node.NodeType.CORFU_SERVER)
                    .build();

            servers.data().forEach(params::add);

            return params;
        }
    }

    @Builder
    @Getter
    class UniverseFixture implements Fixture<UniverseParams> {
        @Default
        private final CorfuGroupFixture group = CorfuGroupFixture.builder().build();

        @Override
        public UniverseParams data() {
            GroupParams groupParams = group.data();
            return UniverseParams.builder()
                    .build()
                    .add(groupParams);
        }
    }
}

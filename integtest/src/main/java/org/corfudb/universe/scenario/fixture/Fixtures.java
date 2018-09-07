package org.corfudb.universe.scenario.fixture;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.Builder;
import lombok.Getter;
import org.corfudb.universe.service.Service.ServiceParams;
import org.slf4j.event.Level;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static lombok.Builder.Default;
import static org.corfudb.universe.cluster.Cluster.ClusterParams;
import static org.corfudb.universe.node.CorfuServer.Mode;
import static org.corfudb.universe.node.CorfuServer.Persistence;
import static org.corfudb.universe.node.CorfuServer.ServerParams;

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
                Mode mode = i == 0 ? Mode.SINGLE : Mode.CLUSTER;

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
    class CorfuServiceFixture implements Fixture<ServiceParams<ServerParams>> {
        @Default
        private final MultipleServersFixture servers = MultipleServersFixture.builder().build();
        @Default
        private final String serviceName = "corfuServer";

        @Override
        public ServiceParams<ServerParams> data() {
            return ServiceParams.<ServerParams>builder()
                    .name(serviceName)
                    .nodes(ImmutableList.copyOf(servers.data()))
                    .build();
        }
    }

    @Builder
    @Getter
    class ClusterFixture implements Fixture<ClusterParams> {
        @Default
        private final CorfuServiceFixture service = CorfuServiceFixture.builder().build();

        @Override
        public ClusterParams data() {
            ServiceParams<ServerParams> serviceParams = service.data();
            return ClusterParams.builder()
                    .services(ImmutableMap.of(serviceParams.getName(), serviceParams))
                    .build();
        }
    }

    @Builder
    @Getter
    class SingleClusterFixture implements Fixture<ClusterParams> {
        @Default
        private final CorfuSingleServiceFixture service = CorfuSingleServiceFixture.builder().build();

        @Override
        public ClusterParams data() {
            ServiceParams<ServerParams> serviceParams = service.data();
            return ClusterParams.builder()
                    .services(ImmutableMap.of(serviceParams.getName(), serviceParams))
                    .build();
        }
    }

    @Builder
    @Getter
    class CorfuSingleServiceFixture implements Fixture<ServiceParams<ServerParams>> {

        @Default
        private final SingleServerFixture servers = SingleServerFixture.builder().build();
        @Default
        private final String serviceName = "corfuServer";

        @Override
        public ServiceParams<ServerParams> data() {
            return ServiceParams.<ServerParams>builder()
                    .name(serviceName)
                    .nodes(ImmutableList.of(servers.data()))
                    .build();
        }
    }
}

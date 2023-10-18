package org.corfudb.infrastructure.management.failuredetector;

import lombok.Getter;
import org.corfudb.infrastructure.NodeNames;
import org.corfudb.infrastructure.datastore.DataStore;
import org.corfudb.infrastructure.management.ClusterAdvisor;
import org.corfudb.infrastructure.management.CompleteGraphAdvisor;
import org.corfudb.infrastructure.management.FileSystemAdvisor;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.infrastructure.management.failuredetector.FailureDetectorDataStore.FailureDetectorDataStoreBuilder;
import org.corfudb.infrastructure.management.failuredetector.FailuresAgent.FailuresAgentBuilder;
import org.corfudb.infrastructure.management.failuredetector.HealingAgent.HealingAgentBuilder;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.BatchProcessorStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.PartitionAttributeStats;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutProbe.LayoutStatus;
import org.corfudb.util.concurrent.SingletonResource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class FailureDetectorTestContext {
    public final String localEndpoint;

    private FailureDetectorDataStoreBuilder fdDataStoreBuilder = FailureDetectorDataStore.builder();

    private ClusterAdvisor advisor;
    private FileSystemAdvisor fsAdvisor;

    private ExecutorService fdWorkerMock = mock(ExecutorService.class);
    private SingletonResource<CorfuRuntime> runtimeMock = mock(SingletonResource.class);

    private HealingAgentBuilder healingAgent;

    private FailuresAgentBuilder failuresAgent;

    private ClusterStateTestContext clusterStateContext;

    public FailureDetectorTestContext(String localEndpoint) {
        this.localEndpoint = localEndpoint;

        fdDataStoreBuilder
                .localEndpoint(localEndpoint)
                .dataStore(mock(DataStore.class));

        advisor = new CompleteGraphAdvisor();
        fsAdvisor = new FileSystemAdvisor();

        FailureDetectorDataStore fdDatastore = fdDataStoreBuilder.build();

        healingAgent = HealingAgent.builder()
                .dataStore(fdDatastore)
                .advisor(advisor)
                .fsAdvisor(fsAdvisor)
                .failureDetectorWorker(fdWorkerMock)
                .runtimeSingleton(runtimeMock);

        failuresAgent = FailuresAgent.builder()
                .fdDataStore(fdDatastore)
                .advisor(advisor)
                .fsAdvisor(fsAdvisor)
                .runtimeSingleton(runtimeMock);

        clusterStateContext = new ClusterStateTestContext(localEndpoint);
    }

    public void setupDataStore(Consumer<FailureDetectorDataStoreBuilder> setupTask) {
        setupTask.accept(fdDataStoreBuilder);
    }

    public void setupHealingAgent(Consumer<HealingAgentBuilder> setupTask) {
        setupTask.accept(healingAgent);
    }

    public HealingAgent getHealingAgentSpy() {
        return spy(healingAgent.build());
    }

    public FailuresAgent getFailuresAgentSpy() {
        return spy(failuresAgent.build());
    }

    public ClusterState getClusterState() {
        return clusterStateContext.getClusterState();
    }

    public PollReport getPollReportMock(ClusterState clusterState) {
        PollReport pollReportMock = mock(PollReport.class);
        when(pollReportMock.getClusterState()).thenReturn(clusterState);
        return pollReportMock;
    }

    public void setupClusterState(Consumer<ClusterStateTestContext> task) {
        task.accept(clusterStateContext);
    }

    public Layout getLayoutMock() {
        Layout layoutMock = mock(Layout.class);
        when(layoutMock.getStatus()).thenReturn(LayoutStatus.empty());
        return layoutMock;
    }

    public static class ClusterStateTestContext {
        private ClusterState.ClusterStateBuilder state = ClusterState.builder();
        private Map<String, NodeState> nodes = new HashMap<>();
        @Getter
        private FileSystemStats fsStats;

        public ClusterStateTestContext(String localEndpoint) {
            state.localEndpoint(localEndpoint);
            fsStats = getFileSystemStats();

            long epoch = 0;

            nodes.put(NodeNames.A, nodeState(NodeNames.A, epoch, Optional.of(fsStats), OK, OK, OK));
            nodes.put(NodeNames.B, nodeState(NodeNames.B, epoch, Optional.of(fsStats), OK, OK, OK));
            nodes.put(NodeNames.C, nodeState(NodeNames.C, epoch, Optional.of(fsStats), OK, OK, OK));
        }

        public void setupNodes(NodeState... states) {
            for (NodeState nodeState : states) {
                nodes.put(nodeState.getConnectivity().getEndpoint(), nodeState);
            }
        }

        public void setupNode(String endpoint, long epoch, Optional<FileSystemStats> fsStats,
                              NodeConnectivity.ConnectionStatus... connectionStates) {
            NodeState node = nodeState(endpoint, epoch, fsStats, connectionStates);
            nodes.put(endpoint, node);
        }

        public void unresponsiveNodes(String... unresponsiveNodes) {
            state.unresponsiveNodes(Arrays.asList(unresponsiveNodes));
        }

        private FileSystemStats getFileSystemStats() {
            return new FileSystemStats(writablePartitionAttrs(), BatchProcessorStats.OK);
        }

        public FileSystemStats getFileSystemStatsWithBpError() {
            return new FileSystemStats(writablePartitionAttrs(), BatchProcessorStats.ERR);
        }

        public FileSystemStats getFileSystemStatsWithReadOnlyDisk() {
            return new FileSystemStats(readOnlyPartitionAttrs(), BatchProcessorStats.ERR);
        }

        public FileSystemStats getFileSystemStatsWithReadOnlyAndBpError() {
            return new FileSystemStats(readOnlyPartitionAttrs(), BatchProcessorStats.ERR);
        }

        public ClusterState getClusterState() {
            return state.nodes(nodes).build();
        }

        public PartitionAttributeStats writablePartitionAttrs() {
            return new PartitionAttributeStats(false, 100, 200);
        }

        public PartitionAttributeStats readOnlyPartitionAttrs() {
            return new PartitionAttributeStats(true, 100, 200);
        }
    }
}

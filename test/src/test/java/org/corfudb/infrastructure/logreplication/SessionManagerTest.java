package org.corfudb.infrastructure.logreplication;

import com.google.common.collect.Sets;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationManager;
import org.corfudb.infrastructure.logreplication.infrastructure.SessionManager;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.msghandlers.LogReplicationServer;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationSinkManager;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientServerRouter;
import org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter;
import org.corfudb.infrastructure.logreplication.transport.server.IServerChannelAdapter;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager.TP_MULTI_SINK;
import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager.TP_SINGLE_SOURCE_SINK;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;

public class SessionManagerTest extends AbstractViewTest {
    private CorfuRuntime corfuRuntime;
    private TopologyDescriptor topology;
    LogReplicationClientServerRouter router;
    CorfuReplicationManager replicationManager;
    LogReplicationServer msgHandler = Mockito.mock(LogReplicationServer.class);
    LogReplicationPluginConfig pluginConfig;
    DefaultClusterManager clusterManager;

    @Before
    public void setUp() throws Exception{
        corfuRuntime = getDefaultRuntime();
        LogReplicationConfigManager configManager = Mockito.mock(LogReplicationConfigManager.class);
        Mockito.doReturn(corfuRuntime).when(configManager).getRuntime();
        Mockito.doNothing().when(msgHandler).updateTopologyConfigId(anyLong());
        Mockito.doReturn(Mockito.mock(LogReplicationSinkManager.class)).when(msgHandler).createSinkManager(any());

        router = Mockito.mock(LogReplicationClientServerRouter.class);
        Mockito.doNothing().when(router).updateTopologyConfigId(anyLong());
        Mockito.doReturn(new HashMap<>()).when(router).getSessionToRequestIdCounter();
        Mockito.doReturn(new HashMap<>()).when(router).getSessionToRemoteClusterDescriptor();
        Mockito.doReturn(new HashMap<>()).when(router).getSessionToLeaderConnectionFuture();


        replicationManager = Mockito.mock(CorfuReplicationManager.class);
        Mockito.doNothing().when(replicationManager).refreshRuntime(any(), any(), anyLong());
        Mockito.doNothing().when(replicationManager).createAndStartRuntime(any(), any());

        pluginConfig = Mockito.mock(LogReplicationPluginConfig.class);

        clusterManager = new DefaultClusterManager();
        DefaultClusterConfig topologyConfig = new DefaultClusterConfig();
        clusterManager.setLocalNodeId(topologyConfig.getSourceNodeUuids().get(0));
    }

    @After
    public void tearDown() {
        corfuRuntime.shutdown();
    }

    /**
     * This test verifies that the outgoing session is established using session manager.
     * It verifies that the in-memory state has captured the outgoing session.
     */
    @Test
    public void testSessionMgrWithOutgoingSession() throws Exception {
        topology = clusterManager.generateSingleSourceSinkTopolgy();
        DefaultClusterConfig.registerWithLR(new CorfuStore(corfuRuntime),
                DefaultClusterConfig.getTopologyTypeToClientModelMap().get(TP_SINGLE_SOURCE_SINK));
        SessionManager sessionManager = new SessionManager(topology, corfuRuntime, replicationManager, router,
                msgHandler, pluginConfig);
        sessionManager.getReplicationContext().setIsLeader(true);
        sessionManager.startClientRegistrationListener();
        while(sessionManager.getOutgoingSessions().isEmpty()) {
            //wait
        }
        sessionManager.refresh(topology);
        int numSinkCluster = topology.getRemoteSinkClusters().size();

        // Verifies that the source cluster has established session with 1 sink clusters.
        Assert.assertEquals(numSinkCluster, sessionManager.getOutgoingSessions().size());
        Assert.assertEquals(0, sessionManager.getIncomingSessions().size());
    }

    /**
     * This test verifies that the incoming session is established using session manager.
     * It also triggers and validates the topology change scenario.
     * Last, tt verifies that the in-memory session state for the incoming session.
     */
    @Test
    public void testSessionMgrTopologyChange() throws Exception {
        topology = clusterManager.generateSingleSourceSinkTopolgy();
        DefaultClusterConfig.registerWithLR(new CorfuStore(corfuRuntime),
                DefaultClusterConfig.getTopologyTypeToClientModelMap().get(TP_SINGLE_SOURCE_SINK));
        SessionManager sessionManager = new SessionManager(topology, corfuRuntime, replicationManager, router,
                msgHandler, pluginConfig);

        sessionManager.getReplicationContext().setIsLeader(true);
        sessionManager.refresh(topology);
        sessionManager.startClientRegistrationListener();
        String sourceClusterId = DefaultClusterConfig.getSourceClusterIds().get(0);
        int numSinkCluster = topology.getRemoteSinkClusters().size();

        // Verifies that the sink cluster has established session with all source clusters (1 in our topology).
        Assert.assertEquals(numSinkCluster, sessionManager.getOutgoingSessions().size());
        Assert.assertEquals(sourceClusterId, topology.getLocalClusterDescriptor().getClusterId());
        Assert.assertEquals(0, sessionManager.getIncomingSessions().size());

        //get sessions from topology1
        Set<LogReplication.LogReplicationSession> sessionsFromTopology1 = new HashSet<>();
        sessionsFromTopology1.addAll(sessionManager.getOutgoingSessions());
        sessionsFromTopology1.addAll(sessionManager.getIncomingSessions());

        // Encounter topology change by introducing a new sink cluster and removing the existing sink cluster.
        TopologyDescriptor newTopology = clusterManager.addAndRemoveSinkFromDefaultTopology();
        sessionManager.refresh(newTopology);
        // get sessions from topology2
        Set<LogReplication.LogReplicationSession> sessionFromTopology2 = new HashSet<>();
        sessionFromTopology2.addAll(sessionManager.getOutgoingSessions());
        sessionFromTopology2.addAll(sessionManager.getIncomingSessions());

        //assert size of incoming and outgoing sessions
        Assert.assertEquals(0, sessionManager.getIncomingSessions().size());
        Assert.assertEquals(newTopology.getRemoteSinkClusters().size(), sessionManager.getOutgoingSessions().size());

        //verify router.stop was called with old stale sessions
        Mockito.verify(router, Mockito.times(1)).stop(Sets.difference(sessionsFromTopology1, sessionFromTopology2));

    }

    /**
     * Test to 'connect' invocation for sessions. A session which has a connection established, should be skipped.
     * This is simulated by removing a cluster and then adding it back to topology.
     * @throws Exception
     */
    @Test
    public void testSessionConnectionOnTopologyChange() throws Exception {
        clusterManager.createSingleSourceMultiSinkTopology();
        // 2nd client registered
        DefaultClusterConfig.registerWithLR(new CorfuStore(corfuRuntime),
                DefaultClusterConfig.getTopologyTypeToClientModelMap().get(TP_MULTI_SINK));

        topology = clusterManager.topologyConfig;
        IClientChannelAdapter clientChannelAdapter = Mockito.mock(IClientChannelAdapter.class);
        IServerChannelAdapter serverChannelAdapter = Mockito.mock(IServerChannelAdapter.class);
        this.router = new LogReplicationClientServerRouter(0L,
                topology.getLocalNodeDescriptor().getClusterId(), topology.getLocalNodeDescriptor().getNodeId(),
                msgHandler, clientChannelAdapter, serverChannelAdapter);

        SessionManager sessionManager = new SessionManager(topology, corfuRuntime, replicationManager, router,
                msgHandler, pluginConfig);
        sessionManager.getReplicationContext().setIsLeader(true);

        sessionManager.refresh(topology);
        sessionManager.connectToRemoteClusters();
        sessionManager.startClientRegistrationListener();
        while(sessionManager.getOutgoingSessions().size() < topology.getRemoteSinkClusters().size()) {
            //wait until the sessions get created
        }
        // sleep for a while so the new sessions are processed.
        TimeUnit.SECONDS.sleep(3);

        Mockito.verify(clientChannelAdapter, Mockito.times(3)).connectAsync(ArgumentMatchers.any(), ArgumentMatchers.any());

        //create a new topology by removing a cluster. This will stop 2 sessions.
        ClusterDescriptor clusterToRemove = topology.getAllClustersInTopology().get(DefaultClusterConfig.getSinkClusterIds().get(0));
        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSinkToModel = new HashMap<>(topology.getRemoteSinkClusterToReplicationModels());
        remoteSinkToModel.remove(clusterToRemove);
        Map<String, ClusterDescriptor> newAllClusters = new HashMap<>(topology.getAllClustersInTopology());
        newAllClusters.remove(clusterToRemove.getClusterId());

        TopologyDescriptor newTopology = new TopologyDescriptor(topology.getTopologyConfigId() + 1, remoteSinkToModel,
                topology.getRemoteSourceClusterToReplicationModels(), newAllClusters,
                topology.getLocalNodeDescriptor().getNodeId());
        sessionManager.refresh(newTopology);
        Assert.assertTrue(router.getConnectedSessions().size() == 2);

        Mockito.clearInvocations(clientChannelAdapter);
        //simulate adding a cluster back to topology. ConnectAsync should be called for the newly added cluster only.
        sessionManager.refresh(topology);
        sessionManager.connectToRemoteClusters();
        Mockito.verify(clientChannelAdapter, Mockito.times(1)).connectAsync(ArgumentMatchers.any(), ArgumentMatchers.any());

    }
}


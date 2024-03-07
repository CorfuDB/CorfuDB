package org.corfudb.infrastructure.logreplication;

import org.corfudb.infrastructure.logreplication.infrastructure.SessionManager;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;


public class SessionManagerTest extends AbstractViewTest {
    private CorfuRuntime corfuRuntime;
    private TopologyDescriptor topology;
    private String pluginFilPath = "src/test/resources/transport/nettyConfig.properties";
    LogReplicationClientServerRouter router;
    CorfuReplicationManager replicationManager;

    LogReplicationServer msgHandler = Mockito.mock(LogReplicationServer.class);
    LogReplicationPluginConfig pluginConfig;


    @Before
    public void setUp() {
        corfuRuntime = getDefaultRuntime();
        LogReplicationConfigManager configManager = Mockito.mock(LogReplicationConfigManager.class);
        Mockito.doReturn(corfuRuntime).when(configManager).getRuntime();

        Mockito.doNothing().when(msgHandler).updateTopologyConfigId(anyLong());
        Mockito.doReturn(Mockito.mock(LogReplicationSinkManager.class)).when(msgHandler).createSinkManager(anyObject());

        router = Mockito.mock(LogReplicationClientServerRouter.class);
        Mockito.doNothing().when(router).updateTopologyConfigId(anyLong());
        Mockito.doReturn(new HashMap<>()).when(router).getSessionToRequestIdCounter();
        Mockito.doReturn(new HashMap<>()).when(router).getSessionToRemoteClusterDescriptor();
        Mockito.doReturn(new HashMap<>()).when(router).getSessionToLeaderConnectionFuture();


        replicationManager = Mockito.mock(CorfuReplicationManager.class);
        Mockito.doNothing().when(replicationManager).refreshRuntime(anyObject(), anyObject(), anyLong());
        Mockito.doNothing().when(replicationManager).updateTopology(anyObject());
        Mockito.doNothing().when(replicationManager).createAndStartRuntime(anyObject(), anyObject(), anyObject());

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
    public void testSessionMgrWithOutgoingSession() {
        DefaultClusterManager defaultClusterManager = new DefaultClusterManager();
        DefaultClusterConfig topologyConfig = new DefaultClusterConfig();
        defaultClusterManager.setLocalNodeId(topologyConfig.getSourceNodeUuids().get(0));
        topology = defaultClusterManager.generateSingleSourceSinkTopolgy();

        SessionManager sessionManager = new SessionManager(topology, corfuRuntime);
        String sourceClusterId = DefaultClusterConfig.getSourceClusterIds().get(0);
        int numSinkCluster = topology.getRemoteSinkClusters().size();

        // Verifies that the source cluster has established session with 1 sink clusters.
        Assert.assertEquals(numSinkCluster, sessionManager.getOutgoingSessions().size());
        Assert.assertEquals(sourceClusterId, topology.getLocalClusterDescriptor().getClusterId());
        Assert.assertEquals(0, sessionManager.getIncomingSessions().size());
    }

    /**
     * This test verifies that the incoming session is established using session manager.
     * It verifies that the in-memory state has captured the incoming session.
     */
    @Test
    public void testSessionMgrWithIncomingSession() {
        DefaultClusterManager defaultClusterManager = new DefaultClusterManager();
        DefaultClusterConfig topologyConfig = new DefaultClusterConfig();
        defaultClusterManager.setLocalNodeId(topologyConfig.getSinkNodeUuids().get(0));
        topology = defaultClusterManager.generateSingleSourceSinkTopolgy();
        
        SessionManager sessionManager = new SessionManager(topology, corfuRuntime);
        String sinkClusterId = DefaultClusterConfig.getSinkClusterIds().get(0);
        int numSourceCluster = topology.getRemoteSourceClusters().size();

        // Verifies that the sink cluster has established session with all source clusters (1 in our topology).
        Assert.assertEquals(0, sessionManager.getOutgoingSessions().size());
        Assert.assertEquals(sinkClusterId, topology.getLocalClusterDescriptor().getClusterId());
        Assert.assertEquals(numSourceCluster, sessionManager.getIncomingSessions().size());
    }
    
    /**
     * This test verifies that the incoming session is established using session manager.
     * It also triggers and validates the topology change scenario.
     * Last, tt verifies that the in-memory session state for the incoming session.
     */
    @Test
    public void testSessionMgrTopologyChange() {
        // Session established using default topology.
        DefaultClusterManager clusterManager = new DefaultClusterManager();
        DefaultClusterConfig topologyConfig = new DefaultClusterConfig();
        clusterManager.setLocalNodeId(topologyConfig.getSourceNodeUuids().get(0));
        topology = clusterManager.generateSingleSourceSinkTopolgy();
        SessionManager sessionManager = new SessionManager(topology, corfuRuntime);
        String sourceClusterId = DefaultClusterConfig.getSourceClusterIds().get(0);
        int numSinkCluster = topology.getRemoteSinkClusters().size();

        // Verifies that the sink cluster has established session with all source clusters (1 in our topology).
        Assert.assertEquals(numSinkCluster, sessionManager.getOutgoingSessions().size());
        Assert.assertEquals(sourceClusterId, topology.getLocalClusterDescriptor().getClusterId());
        Assert.assertEquals(0, sessionManager.getIncomingSessions().size());

        // Encounter topology change by introducing a new sink cluster and removing the existing sink cluster.
        TopologyDescriptor newTopology = clusterManager.addAndRemoveSinkFromDefaultTopology();
        sessionManager.refresh(newTopology);
        Assert.assertEquals(0, sessionManager.getIncomingSessions().size());
        Assert.assertEquals(newTopology.getRemoteSinkClusters().size(), sessionManager.getOutgoingSessions().size());

    }
}


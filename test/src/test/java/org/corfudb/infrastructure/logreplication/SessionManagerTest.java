package org.corfudb.infrastructure.logreplication;

import com.google.common.collect.Sets;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationManager;
import org.corfudb.infrastructure.logreplication.infrastructure.SessionManager;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.msghandlers.LogReplicationServer;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationSinkManager;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientServerRouter;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;

public class SessionManagerTest extends AbstractViewTest {
    private CorfuRuntime corfuRuntime;
    private TopologyDescriptor topology;
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
        Mockito.doReturn(Mockito.mock(LogReplicationSinkManager.class)).when(msgHandler).createSinkManager(any());

        router = Mockito.mock(LogReplicationClientServerRouter.class);
        Mockito.doNothing().when(router).updateTopologyConfigId(anyLong());
        Mockito.doReturn(new HashMap<>()).when(router).getSessionToRequestIdCounter();
        Mockito.doReturn(new HashMap<>()).when(router).getSessionToRemoteClusterDescriptor();
        Mockito.doReturn(new HashMap<>()).when(router).getSessionToLeaderConnectionFuture();


        replicationManager = Mockito.mock(CorfuReplicationManager.class);
        Mockito.doNothing().when(replicationManager).refreshRuntime(any(), any(), anyLong());
        Mockito.doNothing().when(replicationManager).createAndStartRuntime(any(), any(), any());

        pluginConfig = Mockito.mock(LogReplicationPluginConfig.class);
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

        SessionManager sessionManager = new SessionManager(topology, corfuRuntime, replicationManager, router,
                msgHandler, pluginConfig);
        sessionManager.refresh(topology);
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

        SessionManager sessionManager = new SessionManager(topology, corfuRuntime, replicationManager, router,
                msgHandler, pluginConfig);
        sessionManager.refresh(topology);
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

        SessionManager sessionManager = new SessionManager(topology, corfuRuntime, replicationManager, router,
                msgHandler, pluginConfig);
        sessionManager.refresh(topology);
        String sourceClusterId = DefaultClusterConfig.getSourceClusterIds().get(0);
        int numSinkCluster = topology.getRemoteSinkClusters().size();

        // Verifies that the sink cluster has established session with all source clusters (1 in our topology).
        Assert.assertEquals(numSinkCluster, sessionManager.getOutgoingSessions().size());
        Assert.assertEquals(sourceClusterId, topology.getLocalClusterDescriptor().getClusterId());
        Assert.assertEquals(0, sessionManager.getIncomingSessions().size());

        //get sessions from topology1
        Set<LogReplication.LogReplicationSession> sessionsFromTopology1 = new HashSet<>(sessionManager.getSessions());

        // Encounter topology change by introducing a new sink cluster and removing the existing sink cluster.
        TopologyDescriptor newTopology = clusterManager.addAndRemoveSinkFromDefaultTopology();
        sessionManager.refresh(newTopology);
        // get sessions from topology2
        Set<LogReplication.LogReplicationSession> sessionFromTopology2 = new HashSet<>(sessionManager.getSessions());

        //assert size of incoming and outgoing sessions
        Assert.assertEquals(0, sessionManager.getIncomingSessions().size());
        Assert.assertEquals(newTopology.getRemoteSinkClusters().size(), sessionManager.getOutgoingSessions().size());

        //verify router.stop was called with old stale sessions
        Mockito.verify(router, Mockito.times(1)).stop(Sets.difference(sessionsFromTopology1, sessionFromTopology2));

    }
}


package org.corfudb.infrastructure.logreplication;

import com.google.common.collect.Sets;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationManager;
import org.corfudb.infrastructure.logreplication.infrastructure.SessionManager;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.msghandlers.LogReplicationServer;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationSinkManager;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientServerRouter;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Address;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.any;


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
        Mockito.doNothing().when(replicationManager).updateTopology(any());
        Mockito.doNothing().when(replicationManager).createAndStartRuntime(any(), any(), any());

        pluginConfig = Mockito.mock(LogReplicationPluginConfig.class);
    }

    @After
    public void tearDown() {
        corfuRuntime.shutdown();
    }

    /**
     * This test verifies that the outgoing session is established using session manager.
     * It verifies that:
     * (i) when not the leader, the sessions are retrieved from the LR's system tables
     * (ii) when leader, the in-memory state has captured the outgoing session.
     */
    @Test
    public void testSessionMgrWithOutgoingSession() {
        DefaultClusterManager defaultClusterManager = new DefaultClusterManager();
        DefaultClusterConfig topologyConfig = new DefaultClusterConfig();
        defaultClusterManager.setLocalNodeId(topologyConfig.getSourceNodeUuids().get(0));
        topology = defaultClusterManager.generateSingleSourceSinkTopology(false);

        SessionManager sessionManager = new SessionManager(topology, corfuRuntime, replicationManager, router,
                msgHandler, pluginConfig);
        sessionManager.refresh(topology);

        // Local node is not the leader yet. Simulating the leader node creating session by directly adding the session
        // related metadata into the LR's system tables
        String localClusterID = topology.getLocalClusterDescriptor().getClusterId();
        LogReplication.LogReplicationSession outgoingSession = LogReplication.LogReplicationSession.newBuilder()
                .setSourceClusterId(localClusterID)
                .setSinkClusterId(DefaultClusterConfig.getSinkClusterIds().get(0))
                .setSubscriber(LogReplication.ReplicationSubscriber.getDefaultInstance())
                .build();
        addSession(sessionManager, false, outgoingSession);

        Assert.assertEquals(0, sessionManager.getSessions().size());
        Assert.assertEquals(topology.getRemoteSinkClusters().size(), sessionManager.getOutgoingSessions().size());
        Assert.assertEquals(topology.getRemoteSourceClusters().size(), sessionManager.getIncomingSessions().size());
        verifySessions(sessionManager.getOutgoingSessions(), localClusterID, false);
        verifySessions(sessionManager.getIncomingSessions(), localClusterID, true);


        sessionManager.getReplicationContext().setIsLeader(true);
        sessionManager.refresh(topology);
        String sourceClusterId = DefaultClusterConfig.getSourceClusterIds().get(0);

        // Verifies that the leader cluster has established session with 1 sink clusters.
        Assert.assertEquals(topology.getRemoteSinkClusters().size(), sessionManager.getOutgoingSessions().size());
        Assert.assertEquals(sourceClusterId, topology.getLocalClusterDescriptor().getClusterId());
        Assert.assertEquals(topology.getRemoteSourceClusters().size(), sessionManager.getIncomingSessions().size());
        verifySessions(sessionManager.getOutgoingSessions(), localClusterID, false);
        verifySessions(sessionManager.getIncomingSessions(), localClusterID, true);
        Assert.assertEquals(sessionManager.getOutgoingSessions().size() + sessionManager.getIncomingSessions().size(), sessionManager.getSessions().size());
    }

    private void verifySessions(Set<LogReplication.LogReplicationSession> sessions, String localClusterId, boolean incoming) {
        if (incoming) {
            sessions.forEach(session -> {
                Assert.assertEquals(session.getSourceClusterId(), DefaultClusterConfig.getSourceClusterIds().get(0));
                Assert.assertEquals(session.getSinkClusterId(), localClusterId);
            });
        } else {
            sessions.forEach(session -> {
                Assert.assertEquals(session.getSourceClusterId(), localClusterId);
                Assert.assertEquals(session.getSinkClusterId(), DefaultClusterConfig.getSinkClusterIds().get(0));
            });
        }
    }

    private void addSession(SessionManager sessionManager, boolean incoming, LogReplication.LogReplicationSession session) {
        try (TxnContext txn = sessionManager.getMetadataManager().getTxnContext()) {
            sessionManager.getMetadataManager().initializeMetadata(txn, session, incoming, 1);
            txn.commit();
        }
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
        topology = defaultClusterManager.generateSingleSourceSinkTopology(false);

        SessionManager sessionManager = new SessionManager(topology, corfuRuntime, replicationManager, router,
                msgHandler, pluginConfig);
        sessionManager.refresh(topology);
        // Local node is not the leader yet. Simulating the leader node creating session by directly adding the session
        // related metadata into the LR's system tables
        String localClusterID = topology.getLocalClusterDescriptor().getClusterId();
        LogReplication.LogReplicationSession incomingSession = LogReplication.LogReplicationSession.newBuilder()
                .setSourceClusterId(DefaultClusterConfig.getSourceClusterIds().get(0))
                .setSinkClusterId(localClusterID)
                .setSubscriber(LogReplication.ReplicationSubscriber.getDefaultInstance())
                .build();
        addSession(sessionManager, true, incomingSession);

        Assert.assertEquals(0, sessionManager.getSessions().size());
        Assert.assertEquals(topology.getRemoteSinkClusters().size(), sessionManager.getOutgoingSessions().size());
        Assert.assertEquals(topology.getRemoteSourceClusters().size(), sessionManager.getIncomingSessions().size());
        verifySessions(sessionManager.getOutgoingSessions(), localClusterID, false);
        verifySessions(sessionManager.getIncomingSessions(), localClusterID, true);


        sessionManager.getReplicationContext().setIsLeader(true);
        sessionManager.refresh(topology);
        String sinkClusterId = DefaultClusterConfig.getSinkClusterIds().get(0);

        // Verifies that the sink cluster has established session with all source clusters (1 in our topology).
        Assert.assertEquals(topology.getRemoteSinkClusters().size(), sessionManager.getOutgoingSessions().size());
        Assert.assertEquals(sinkClusterId, topology.getLocalClusterDescriptor().getClusterId());
        Assert.assertEquals(topology.getRemoteSourceClusters().size(), sessionManager.getIncomingSessions().size());
        Assert.assertEquals(sessionManager.getOutgoingSessions().size() + sessionManager.getIncomingSessions().size(), sessionManager.getSessions().size());
        verifySessions(sessionManager.getOutgoingSessions(), localClusterID, false);
        verifySessions(sessionManager.getIncomingSessions(), localClusterID, true);
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
        topology = clusterManager.generateSingleSourceSinkTopology(false);

        SessionManager sessionManager = new SessionManager(topology, corfuRuntime, replicationManager, router,
                msgHandler, pluginConfig);
        sessionManager.getReplicationContext().setIsLeader(true);
        sessionManager.refresh(topology);
        String sourceClusterId = DefaultClusterConfig.getSourceClusterIds().get(0);
        String localClusterId = topology.getLocalClusterDescriptor().getClusterId();

        // Verifies that the sink cluster has established session with all source clusters (1 in our topology).
        Assert.assertEquals(topology.getRemoteSinkClusters().size(), sessionManager.getOutgoingSessions().size());
        Assert.assertEquals(sourceClusterId, topology.getLocalClusterDescriptor().getClusterId());
        Assert.assertEquals(topology.getRemoteSourceClusters().size(), sessionManager.getIncomingSessions().size());
        Assert.assertEquals(sessionManager.getOutgoingSessions().size() + sessionManager.getIncomingSessions().size(), sessionManager.getSessions().size());
        verifySessions(sessionManager.getOutgoingSessions(), localClusterId, false);
        verifySessions(sessionManager.getIncomingSessions(), localClusterId, true);

        //get sessions from topology1
        Set<LogReplication.LogReplicationSession> sessionsFromTopology1 = new HashSet<>(sessionManager.getSessions());

        // Encounter topology change by introducing a new sink cluster and removing the existing sink cluster.
        TopologyDescriptor newTopology = clusterManager.addAndRemoveSinkFromDefaultTopology();
        sessionManager.refresh(newTopology);
        // get sessions from topology2
        Set<LogReplication.LogReplicationSession> sessionFromTopology2 = new HashSet<>(sessionManager.getSessions());

        //assert size of incoming and outgoing sessions
        Assert.assertEquals(newTopology.getRemoteSourceClusters().size(), sessionManager.getIncomingSessions().size());
        Assert.assertEquals(newTopology.getRemoteSinkClusters().size(), sessionManager.getOutgoingSessions().size());
        Assert.assertEquals(sessionManager.getOutgoingSessions()
                .stream()
                .filter(session -> !session.getSourceClusterId().equals(localClusterId))
                .collect(Collectors.toSet()).size(), 0);
        Assert.assertEquals(sessionManager.getOutgoingSessions()
                .stream()
                .filter(session -> !DefaultClusterConfig.getSinkClusterIds().contains(session.getSinkClusterId()))
                .collect(Collectors.toSet()).size(), 0);
        Assert.assertEquals(sessionManager.getOutgoingSessions().size() + sessionManager.getIncomingSessions().size(), sessionManager.getSessions().size());


        //verify router.stop was called with old stale sessions
        Mockito.verify(router, Mockito.times(1)).stop(Sets.difference(sessionsFromTopology1, sessionFromTopology2));

    }
}


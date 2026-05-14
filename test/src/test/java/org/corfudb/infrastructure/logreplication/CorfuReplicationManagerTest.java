package org.corfudb.infrastructure.logreplication;

import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationManager;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CorfuReplicationManager}.
 */
public class CorfuReplicationManagerTest {

    private static final String CURRENT_STANDBY_ID = "current-standby";
    private static final String GHOST_CLUSTER_ID_1  = "ghost-cluster-1";
    private static final String GHOST_CLUSTER_ID_2  = "ghost-cluster-2";

    @Mock private LogReplicationMetadataManager metadataManager;
    @Mock private TopologyDescriptor topology;
    @Mock private LogReplicationContext context;
    @Mock private LogReplicationConfigManager configManager;

    private CorfuReplicationManager replicationManager;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        when(topology.getStandbyClusters()).thenReturn(
                Collections.singletonMap(CURRENT_STANDBY_ID, new ClusterDescriptor(CURRENT_STANDBY_ID)));

        replicationManager = new CorfuReplicationManager(context, null, metadataManager, null, null, configManager);
        replicationManager.setTopology(topology);
    }

    /**
     * Ghost entries — rows for clusters decommissioned before the current leader started — must be
     * removed from the LogReplicationStatus table when {@link CorfuReplicationManager#reconcileStatusTable()}
     * is called.  Without the call in {@code CorfuReplicationDiscoveryService.onLeadershipAcquire()},
     * ghost entries persist and {@code queryReplicationStatus()} can non-deterministically return a
     * stale STOPPED status, blocking switchover indefinitely.
     */
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testReconcileStatusTableRemovesGhostEntries() {
        Map entries = new HashMap();
        entries.put(CURRENT_STANDBY_ID, null);
        entries.put(GHOST_CLUSTER_ID_1,  null);
        entries.put(GHOST_CLUSTER_ID_2,  null);
        doReturn(entries).when(metadataManager).getReplicationRemainingEntries();

        replicationManager.reconcileStatusTable();

        verify(metadataManager, never()).removeFromStatusTable(CURRENT_STANDBY_ID);
        verify(metadataManager).removeFromStatusTable(GHOST_CLUSTER_ID_1);
        verify(metadataManager).removeFromStatusTable(GHOST_CLUSTER_ID_2);
    }

    /**
     * When the status table already contains only current standbys, reconciliation must be a no-op.
     */
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testReconcileStatusTableIsNoOpWhenTableIsClean() {
        Map entries = new HashMap();
        entries.put(CURRENT_STANDBY_ID, null);
        doReturn(entries).when(metadataManager).getReplicationRemainingEntries();

        replicationManager.reconcileStatusTable();

        verify(metadataManager, never()).removeFromStatusTable(any());
    }
}

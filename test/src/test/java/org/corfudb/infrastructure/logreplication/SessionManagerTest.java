package org.corfudb.infrastructure.logreplication;

import org.corfudb.infrastructure.logreplication.infrastructure.SessionManager;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

public class SessionManagerTest extends AbstractViewTest {
    private CorfuRuntime corfuRuntime;
    private LogReplicationConfigManager configManager;
    private TestUtils utils;
    private List<CorfuMessage.LogReplicationSession> sessions = DefaultClusterConfig.getSessions();
    private CorfuMessage.LogReplicationSession defaultSession = sessions.get(0);
    private TopologyDescriptor topology = new DefaultClusterManager().generateDefaultValidConfig();

    @Before
    public void setUp() {
        corfuRuntime = getDefaultRuntime();

        configManager = Mockito.mock(LogReplicationConfigManager.class);

        Mockito.doReturn(corfuRuntime).when(configManager).getRuntime();
        utils = new TestUtils();
    }

    @After
    public void tearDown() {
        corfuRuntime.shutdown();
    }

    /**
     * This test verifies that the timestamp of the last log entry processed during LogEntry sync is correctly
     * updated in the metadata table on the Sink cluster
     */
    @Test
    public void testSessionMgrAfterLogEntrySync() {
        SessionManager sessionManager = new SessionManager(topology, corfuRuntime, null, null);
        Assert.assertNotNull(sessionManager);
        // TODO: Add further verification.
    }
}


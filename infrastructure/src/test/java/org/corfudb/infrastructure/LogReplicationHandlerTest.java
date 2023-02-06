package org.corfudb.infrastructure;

import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClient;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationHandler;
import org.corfudb.runtime.clients.IClientRouter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.UUID;
import static org.mockito.Mockito.mock;

public class LogReplicationHandlerTest {

    private LogReplicationHandler lrHandler;

    private IClientRouter router;

    private UUID uuid;

    @Before
    public void setup() {
        lrHandler = new LogReplicationHandler();
        router = mock(IClientRouter.class);
        uuid = UUID.randomUUID();
        lrHandler.setRouter(router);
    }

    /**
     * Test the getClient method.
     */
    @Test
    public void testGetClientWithEpoch() {
        long epoch = 10;
        LogReplicationClient lrClient = new LogReplicationClient(router, epoch);
        Assert.assertEquals(lrClient.getRouter(), lrHandler.getClient(epoch,uuid).getRouter());
        Assert.assertEquals(lrClient.getEpoch(), lrHandler.getClient(epoch,uuid).getEpoch());
    }
}

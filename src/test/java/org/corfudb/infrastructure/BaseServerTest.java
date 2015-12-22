package org.corfudb.infrastructure;

import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 12/14/15.
 */
public class BaseServerTest extends AbstractServerTest {

    BaseServer server;

    @Before
    public void setupTest() {
        server = new BaseServer(this.router);
        this.setServer(server);
    }

    @Override
    public IServer getDefaultServer() {
        return null;
    }

    @Test
    public void testPing()
    {
        sendMessage(new CorfuMsg(CorfuMsg.CorfuMsgType.PING));
        assertThat(getLastMessage().getMsgType())
            .isEqualTo(CorfuMsg.CorfuMsgType.PONG);
    }
}

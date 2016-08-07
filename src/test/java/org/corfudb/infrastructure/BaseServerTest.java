package org.corfudb.infrastructure;

import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.CorfuSetEpochMsg;
import org.corfudb.protocols.wireprotocol.JSONPayloadMsg;
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
        server = new BaseServer();
        this.setServer(server);
    }

    @Override
    public AbstractServer getDefaultServer() {
        return null;
    }

    @Test
    public void testPing() {
        sendMessage(new CorfuMsg(CorfuMsg.CorfuMsgType.PING));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.PONG);
    }

    @Test
    public void canSetServerEpoch() {
        sendMessage(new CorfuPayloadMsg<>(CorfuMsg.CorfuMsgType.SET_EPOCH, 6L));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.ACK);
        sendMessage(new CorfuMsg(CorfuMsg.CorfuMsgType.PING));
        assertThat(getLastMessage().getEpoch())
                .isEqualTo(6L);

    }

    @Test
    public void shutdownServerDoesNotRespond() {
        server.shutdown();
        sendMessage(new CorfuMsg(CorfuMsg.CorfuMsgType.PING));
        assertThat(getLastMessage())
                .isNull();
    }
}

package org.corfudb.infrastructure;

import org.assertj.core.api.Assertions;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.junit.Test;

/**
 * Created by mwei on 12/14/15.
 */
public class BaseServerTest extends AbstractServerTest {

    BaseServer bs;

    @Override
    public AbstractServer getDefaultServer() {
        if (bs == null) {
            bs = new BaseServer(ServerContextBuilder.defaultTestContext(0));
        }
        return bs;
    }

    @Test
    public void testPing() {
        sendMessage(new CorfuMsg(CorfuMsgType.PING));
        Assertions.assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.PONG);
    }

    @Test
    public void shutdownServerDoesNotRespond() {
        getDefaultServer().shutdown();
        sendMessage(new CorfuMsg(CorfuMsgType.PING));
        Assertions.assertThat(getLastMessage().getMsgType())
            .isEqualTo(CorfuMsgType.ERROR_SHUTDOWN_EXCEPTION);
    }
}

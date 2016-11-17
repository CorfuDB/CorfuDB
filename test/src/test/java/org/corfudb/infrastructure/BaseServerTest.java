package org.corfudb.infrastructure;

import org.assertj.core.api.Assertions;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 12/14/15.
 */
public class BaseServerTest extends AbstractServerTest {

    BaseServer bs;

    @Override
    public AbstractServer getDefaultServer() {
        if (bs == null) {
            bs = new BaseServer();
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
        Assertions.assertThat(getLastMessage())
                .isNull();
        sendMessage(new CorfuMsg(CorfuMsgType.PING));
        Assertions.assertThat(getLastMessage())
                .isNull();
    }
}

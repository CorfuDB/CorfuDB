package org.corfudb.infrastructure;

import org.corfudb.protocols.service.CorfuProtocolBase;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        CompletableFuture<Boolean> res = sendRequest(new CorfuMsg(CorfuMsgType.PING));
        assertThat(res.join()).isTrue();
    }

    @Test
    public void testProtoPing() {
        CompletableFuture<Boolean> res = sendRequest(CorfuProtocolBase.getPingRequestMsg(), true, true);
        assertThat(res.join()).isTrue();
    }

    @Test
    public void testProtoSeal() {
        // Set the ignoreClusterId and ignoreEpoch to be true for testing purpose.
        CompletableFuture<Boolean> res = sendRequest(CorfuProtocolBase.getSealRequestMsg(1L), true, true);
        assertThat(res.join()).isTrue();
        assertThatThrownBy(() -> {
             sendRequest(CorfuProtocolBase.getSealRequestMsg(0L), true, true).join();
        }).hasCauseInstanceOf(WrongEpochException.class);
    }

}

package org.corfudb.runtime.clients;

import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.junit.Test;

import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 6/29/16.
 */
public class TestClientRouterTest extends AbstractCorfuTest {

    @Test
    public void testRuleDropsMessages() {
        TestClientRouter tcr = new TestClientRouter();
        tcr.addServer(new BaseServer(tcr));
        BaseClient bc = new BaseClient();
        tcr.addClient(bc);

        assertThat(bc.pingSync())
                .isTrue();

        tcr.serverToClientRules.add(new TestClientRule()
                .always()
                .drop());

        assertThat(bc.pingSync())
                .isFalse();
    }

    @Test
    public void onlyDropEpochChangeMessages() {
        TestClientRouter tcr = new TestClientRouter();
        tcr.addServer(new BaseServer(tcr));
        BaseClient bc = new BaseClient();
        tcr.addClient(bc);

        tcr.clientToServerRules.add(new TestClientRule()
                .matches(x -> x.getMsgType().equals(CorfuMsg.CorfuMsgType.SET_EPOCH))
                .drop());

        assertThat(bc.pingSync())
                .isTrue();

        assertThatThrownBy(() -> bc.setRemoteEpoch(9L).get())
                .hasCauseInstanceOf(TimeoutException.class);
    }
}

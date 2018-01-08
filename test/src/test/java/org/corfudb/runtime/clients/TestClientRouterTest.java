package org.corfudb.runtime.clients;

import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.TestServerRouter;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
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
        TestServerRouter tsr = new TestServerRouter();
        BaseServer bs = new BaseServer(ServerContextBuilder.defaultTestContext(0));
        tsr.addServer(bs);
        TestClientRouter tcr = new TestClientRouter(tsr);

        BaseClient bc = new BaseClient();
        tcr.addClient(bc);

        assertThat(bc.pingSync())
                .isTrue();

        tcr.rules.add(new TestRule()
                .always()
                .drop());

        assertThat(bc.pingSync())
                .isFalse();
    }

    @Test
    public void onlyDropEpochChangeMessages() {
        TestServerRouter tsr = new TestServerRouter();
        BaseServer bs = new BaseServer(ServerContextBuilder.defaultTestContext(0));
        tsr.addServer(bs);
        TestClientRouter tcr = new TestClientRouter(tsr);

        BaseClient bc = new BaseClient();
        tcr.addClient(bc);

        tcr.rules.add(new TestRule()
                .matches(x -> x.getMsgType().equals(CorfuMsgType.SET_EPOCH))
                .drop());

        assertThat(bc.pingSync())
                .isTrue();

        final long NEW_EPOCH = 9L;
        assertThatThrownBy(() -> bc.setRemoteEpoch(NEW_EPOCH).get())
                .hasCauseInstanceOf(TimeoutException.class);
    }

    @Test
    public void doesNotUpdateEpochBackward() throws Exception {
        TestServerRouter tsr = new TestServerRouter();
        BaseServer bs = new BaseServer(ServerContextBuilder.defaultTestContext(0));
        tsr.addServer(bs);
        TestClientRouter tcr = new TestClientRouter(tsr);

        BaseClient bc = new BaseClient();
        tcr.addClient(bc);

        long currentEpoch = tcr.getEpoch();
        tcr.setEpoch(currentEpoch + 1);
        assertThat(tcr.getEpoch()).isEqualTo(currentEpoch + 1);

        currentEpoch = tcr.getEpoch();
        tcr.setEpoch(currentEpoch - 1);
        assertThat(tcr.getEpoch()).isNotEqualTo(currentEpoch - 1).isEqualTo(currentEpoch);

    }
}

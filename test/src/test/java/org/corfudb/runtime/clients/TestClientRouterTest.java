package org.corfudb.runtime.clients;

import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.TestServerRouter;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 6/29/16.
 */
public class TestClientRouterTest extends AbstractCorfuTest {

    private BaseClient bc;
    private TestClientRouter tcr;

    @Before
    public void setupRouter() {
        TestServerRouter tsr = new TestServerRouter();
        BaseServer bs = new BaseServer(ServerContextBuilder.defaultTestContext(0));
        tsr.addServer(bs);
        tcr = new TestClientRouter(tsr);
        BaseHandler baseHandler = new BaseHandler();
        tcr.addClient(baseHandler);
        bc = new BaseClient(tcr, 0L);

    }

    @Test
    public void testRuleDropsMessages() {
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
        tcr.rules.add(new TestRule()
                .matches(x -> x.getMsgType().equals(CorfuMsgType.SEAL))
                .drop());

        assertThat(bc.pingSync())
                .isTrue();

        final long NEW_EPOCH = 9L;
        assertThatThrownBy(() -> bc.sealRemoteServer(NEW_EPOCH).get())
                .hasCauseInstanceOf(TimeoutException.class);
    }
}

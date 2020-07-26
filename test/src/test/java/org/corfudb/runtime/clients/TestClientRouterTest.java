package org.corfudb.runtime.clients;

import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.TestServerRouter;
import org.corfudb.infrastructure.server.CorfuServerStateMachine;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 6/29/16.
 */
public class TestClientRouterTest extends AbstractCorfuTest {

    private BaseClient bc;
    private TestClientRouter tcr;
    private final CorfuServerStateMachine serverSm = Mockito.mock(CorfuServerStateMachine.class);

    @Before
    public void setupRouter() {
        TestServerRouter tsr = new TestServerRouter();
        BaseServer bs = new BaseServer(ServerContextBuilder.defaultTestContext(0), serverSm);
        tsr.addServer(bs);
        tcr = new TestClientRouter(tsr);
        BaseHandler baseHandler = new BaseHandler();
        tcr.addClient(baseHandler);
        bc = new BaseClient(tcr, 0L, UUID.fromString("00000000-0000-0000-0000-000000000000"));

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

    @Override
    public void close() throws Exception {
        //empty
    }
}

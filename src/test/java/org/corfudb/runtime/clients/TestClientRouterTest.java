package org.corfudb.runtime.clients;

import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.BaseServer;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
}

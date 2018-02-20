package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.util.CFUtils;
import org.junit.Test;

import java.util.Set;

/**
 * Created by mwei on 7/27/16.
 */
public class BaseClientTest extends AbstractClientTest {

    BaseSenderClient client;

    @Override
    Set<AbstractServer> getServersForTest() {
        return new ImmutableSet.Builder<AbstractServer>()
                .add(new BaseServer(ServerContextBuilder.defaultTestContext(0)))
                .build();
    }

    @Override
    Set<IClient> getClientsForTest() {
        BaseClient baseClient = new BaseClient();
        client = new BaseSenderClient(router, 0L);
        return new ImmutableSet.Builder<IClient>()
                .add(baseClient)
                .build();
    }

    @Test
    public void canGetVersionInfo() {
        CFUtils.getUninterruptibly(client.getVersionInfo());
    }
}

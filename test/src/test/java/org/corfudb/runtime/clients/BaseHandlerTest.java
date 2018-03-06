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
public class BaseHandlerTest extends AbstractClientTest {

    BaseClient client;

    @Override
    Set<AbstractServer> getServersForTest() {
        return new ImmutableSet.Builder<AbstractServer>()
                .add(new BaseServer(ServerContextBuilder.defaultTestContext(0)))
                .build();
    }

    @Override
    Set<IClient> getClientsForTest() {
        BaseHandler baseHandler = new BaseHandler();
        client = new BaseClient(router, 0L);
        return new ImmutableSet.Builder<IClient>()
                .add(baseHandler)
                .build();
    }

    @Test
    public void canGetVersionInfo() {
        CFUtils.getUninterruptibly(client.getVersionInfo());
    }
}

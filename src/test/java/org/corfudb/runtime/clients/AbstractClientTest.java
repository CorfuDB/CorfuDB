package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableMap;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.IServer;
import org.junit.Before;

import java.util.Map;
import java.util.Set;

/**
 * Created by mwei on 12/13/15.
 */
public abstract class AbstractClientTest extends AbstractCorfuTest {

    TestClientRouter router;

    @Before
    public void resetTest()
    {
        router = new TestClientRouter();
        getServersForTest().stream().forEach(router::addServer);
        getClientsForTest().stream().forEach(router::addClient);
    }

    abstract Set<IServer> getServersForTest();
    abstract Set<IClient> getClientsForTest();

    public Map<String,Object> defaultOptionsMap()
    {
        return new ImmutableMap.Builder<String,Object>()
                .put("--initial-token", "0")
                .put("--memory", true)
                .put("--single", false)
                .put("--max-cache", "256M")
                .build();
    }
}

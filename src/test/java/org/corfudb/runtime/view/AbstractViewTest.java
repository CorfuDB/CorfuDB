package org.corfudb.runtime.view;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.IServer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.clients.TestClientRouter;
import org.junit.Before;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by mwei on 12/22/15.
 */
public abstract class AbstractViewTest extends AbstractCorfuTest {

    @Getter
    CorfuRuntime runtime;

    Map<String, TestClientRouter> routerMap;

    Map<String, Set<IServer>> serverMap;

    @Before
    public void resetTests()
    {
        routerMap.clear();
        serverMap.clear();
        runtime.parseConfigurationString(getDefaultConfigurationString());
    }

    /** Wire all registered servers to the correct router.
     *  This function must be called prior to running the actual test logic.
     */
    public void wireRouters()
    {
        serverMap.keySet().stream()
                .map(address -> {
                    TestClientRouter r = new TestClientRouter();
                    r.setAddress(address);
                    r.addClient(new LayoutClient())
                            .addClient(new SequencerClient())
                            .addClient(new LogUnitClient())
                            .start();
                    serverMap.get(address).stream()
                            .forEach(r::addServer);
                    return r;
                })
                .forEach(r -> routerMap.put(r.getAddress(), r));
    }

    public void addServerForTest(String address, IServer server) {
        serverMap.compute(address, (k, v) -> {
            Set<IServer> out = v;
            if (v == null) {
                out = new HashSet<IServer>();
            }
            out.add(server);
            return out;
        });
    }

    public AbstractViewTest()
    {
        runtime = new CorfuRuntime();
        routerMap = new HashMap<>();
        serverMap = new HashMap<>();
        runtime.setGetRouterFunction(routerMap::get);
    }

    public abstract String getDefaultConfigurationString();

    public String getDefaultEndpoint()
    {
        return "localhost:9000";
    }

    public Map<String,Object> defaultOptionsMap()
    {
        return new ImmutableMap.Builder<String,Object>()
                .put("--initial-token", "0")
                .put("--memory", true)
                .put("--single", true)
                .put("--address", getDefaultEndpoint().split(":")[0])
                .put("<port>", getDefaultEndpoint().split(":")[1])
                .build();
    }
}

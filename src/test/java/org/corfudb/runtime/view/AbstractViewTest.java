package org.corfudb.runtime.view;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.IServer;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.*;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.junit.Before;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        runtime.parseConfigurationString(getDefaultConfigurationString())
                .setCacheDisabled(true); // Disable cache during unit tests to fully stress the system.
        runtime.getAddressSpaceView().resetCaches();
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
                            .addClient(new BaseClient())
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

    public void removeServerForTest(String address, IServer server) {
        serverMap.compute(address, (k, v) -> {
            Set<IServer> out = v;
            if (v == null) {
                out = new HashSet<IServer>();
            }
            out.remove(server);
            return out;
        });
    }

    public CorfuRuntime getDefaultRuntime() {
        // default layout is chain replication.
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new LogUnitServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));
        wireRouters();

        return getRuntime().connect();
    }

    public AbstractViewTest()
    {
        runtime = new CorfuRuntime();
        routerMap = new HashMap<>();
        serverMap = new HashMap<>();
        runtime.setGetRouterFunction(routerMap::get);
    }

    public String getDefaultConfigurationString() {return getDefaultEndpoint();}

    public String getDefaultEndpoint()
    {
        return "localhost:9000";
    }

    public String getEndpoint(long port)
    {
        return "localhost:" + port;
    }

    public Map<String,Object> defaultOptionsMap()
    {
        return new ImmutableMap.Builder<String,Object>()
                .put("--initial-token", "0")
                .put("--memory", true)
                .put("--single", true)
                .put("--max-cache", "256M")
                .put("--address", getDefaultEndpoint().split(":")[0])
                .put("<port>", getDefaultEndpoint().split(":")[1])
                .build();
    }

    public void setLayout(Layout l)
            throws QuorumUnreachableException, OutrankedException
    {
        getRuntime().getLayoutView().updateLayout(l, l.epoch);
        getRuntime().invalidateLayout();
        assertThat(getRuntime().getLayoutView().getLayout().epoch)
                .isEqualTo(l.epoch);
    }
}

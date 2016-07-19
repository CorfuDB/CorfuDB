package org.corfudb.runtime.view;

import lombok.Data;
import lombok.Getter;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.*;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.LayoutMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.*;
import org.junit.Before;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 12/22/15.
 */
public abstract class AbstractViewTest extends AbstractCorfuTest {

    @Getter
    CorfuRuntime runtime;

    final Map<String, TestServer> testServerMap = new ConcurrentHashMap<>();

    final Map<CorfuRuntime, Map<String, TestClientRouter>>
            runtimeRouterMap = new ConcurrentHashMap<>();

    public AbstractViewTest() {
        // Force all new CorfuRuntimes to override the getRouterFn
        CorfuRuntime.overrideGetRouterFunction = this::getRouterFunction;
        runtime = new CorfuRuntime(getDefaultEndpoint());
    }

    private IClientRouter getRouterFunction(CorfuRuntime runtime, String endpoint) {
        runtimeRouterMap.putIfAbsent(runtime, new ConcurrentHashMap<>());
        if (!endpoint.startsWith("test:")) {
            throw new RuntimeException("Unsupported endpoint in test: " + endpoint);
        }
        return runtimeRouterMap.get(runtime).computeIfAbsent(endpoint,
                x -> {
                    TestClientRouter tcn =
                            new TestClientRouter(testServerMap.get(endpoint).getServerRouter());
                    tcn.addClient(new BaseClient())
                            .addClient(new SequencerClient())
                            .addClient(new LayoutClient())
                            .addClient(new LogUnitClient());
                    return tcn;
                }
        );
    }

    @Before
    public void resetTests() {
        testServerMap.clear();
        runtime.parseConfigurationString(getDefaultConfigurationString())
                .setCacheDisabled(true); // Disable cache during unit tests to fully stress the system.
        runtime.getAddressSpaceView().resetCaches();
    }

    @Data
    static class TestServer {
        BaseServer baseServer;
        SequencerServer sequencerServer;
        LayoutServer layoutServer;
        LogUnitServer logUnitServer;
        TestServerRouter serverRouter;
        int port;

        TestServer(Map<String, Object> optsMap)
        {
            serverRouter = new TestServerRouter();
            baseServer = new BaseServer(serverRouter);
            sequencerServer = new SequencerServer(optsMap);
            layoutServer = new LayoutServer(optsMap, serverRouter);
            logUnitServer = new LogUnitServer(optsMap);

            serverRouter.addServer(baseServer);
            serverRouter.addServer(sequencerServer);
            serverRouter.addServer(layoutServer);
            serverRouter.addServer(logUnitServer);
        }

        TestServer(int port)
        {
            this(ServerConfigBuilder.defaultConfig(port));
        }

        void addToTest(int port, AbstractViewTest test) {

            if (test.testServerMap.putIfAbsent("test:" + port, this) != null) {
                throw new RuntimeException("Server already registered at port " + port);
            }

        }
    }


    public void addServer(int port, Map<String, Object> config) {
        new TestServer(config).addToTest(port, this);
    }

    public void addServer(int port) {
        new TestServer(new ServerConfigBuilder().setSingle(false).setPort(port).build()).addToTest(port, this);
    }

    public void addSingleServer(int port) {
        new TestServer(port).addToTest(port, this);
    }


    public TestServer getServer(int port) {
        return testServerMap.get("test:" + port);
    }

    public LogUnitServer getLogUnit(int port) {
        return getServer(port).getLogUnitServer();
    }

    public SequencerServer getSequencer(int port) {
        return getServer(port).getSequencerServer();
    }

    public LayoutServer getLayoutServer(int port) {
        return getServer(port).getLayoutServer();
    }

    public BaseServer getBaseServer(int port) {
        return getServer(port).getBaseServer();
    }

    public void bootstrapAllServers(Layout l)
    {
        testServerMap.entrySet().parallelStream()
                .forEach(e -> {
                    e.getValue().layoutServer.reset();
                    e.getValue().layoutServer
                            .handleMessage(new LayoutMsg(l, CorfuMsg.CorfuMsgType.LAYOUT_BOOTSTRAP),
                                    null, e.getValue().serverRouter);
                });
    }

    public CorfuRuntime getDefaultRuntime() {
        addSingleServer(9000);
        return getRuntime().connect();
    }

    public CorfuRuntime wireExistingRuntimeToTest(CorfuRuntime runtime) {
        runtime.layoutServers.add(getDefaultEndpoint());
        return runtime;
    }

    public void clearClientRules() {
        clearClientRules(getRuntime());
    }

    public void clearClientRules(CorfuRuntime r) {
        runtimeRouterMap.get(r).values().forEach(x -> x.rules.clear());
    }

    public void addClientRule(TestRule rule) {
        addClientRule(getRuntime(), rule);
    }

    public void addClientRule(CorfuRuntime r, TestRule rule) {
        runtimeRouterMap.get(r).values().forEach(x -> x.rules.add(rule));
    }

    public void clearServerRules(int port) {
        getServer(port).getServerRouter().rules.clear();
    }

    public void addServerRule(int port, TestRule rule) {
        getServer(port).getServerRouter().rules.add(rule);
    }


    public String getDefaultConfigurationString() {
        return getDefaultEndpoint();
    }

    public String getDefaultEndpoint() {
        return "test:9000";
    }

    public String getEndpoint(long port) {
        return "test:" + port;
    }
}

package org.corfudb.runtime.view;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.ManagementServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.TestServerRouter;
import org.corfudb.infrastructure.management.FailureDetector;
import org.corfudb.protocols.service.CorfuProtocolMessage;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.clients.BaseHandler;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutHandler;
import org.corfudb.runtime.clients.LogUnitHandler;
import org.corfudb.runtime.clients.ManagementHandler;
import org.corfudb.runtime.clients.SequencerHandler;
import org.corfudb.runtime.clients.TestClientRouter;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.util.LambdaUtils.ThrowableConsumer;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.MockitoAnnotations;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.protocols.CorfuProtocolCommon.DEFAULT_UUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getBootstrapLayoutRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getBootstrapManagementRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getBootstrapSequencerRequestMsg;

/**
 * This class serves as a base class for most higher-level Corfu unit tests
 * providing several helper functions to reduce boilerplate code.
 * <p>
 * For most tests, a CorfuRuntime can be obtained by calling getDefaultRuntime().
 * This instantiates a single-node in-memory Corfu server at port 9000, already
 * bootstrapped. If getDefaultRuntime() is not called, then no servers are
 * started.
 * <p>
 * For all other tests, servers can be started using the addServer(port, options)
 * function. The bootstrapAllServers(layout) function can be used to
 * bootstrap the servers with a specific layout. These servers can be referred
 * to by a CorfuRuntime using the "test:<port number>" convention. For example,
 * calling new CorfuRuntime("test:9000"); will connect a CorfuRuntime to the
 * test server at port 9000.
 * <p>
 * To access servers, call the getLogUnit(port), getLayoutServer(port) and
 * getSequencer(port). This allows access to the server class public fields
 * and methods.
 * <p>
 * In addition to simulating Corfu servers, this class also permits installing
 * special rules, which can be used to simulate failures or reorder messages.
 * To install, use the addClientRule(testRule) and addServerRule(testRule)
 * methods.
 * <p>
 * Created by mwei on 12/22/15.
 */
@Slf4j
public abstract class AbstractViewTest extends AbstractCorfuTest {

    private static final int QUIET_PERIOD = 100;
    private static final int TIMEOUT = 300;
    protected static final int MVO_CACHE_SIZE = 5000;

    /** The runtime generated by default, by getDefaultRuntime(). */
    @Getter
    CorfuRuntime runtime;

    private final ConcurrentLinkedQueue<CorfuRuntime> runtimes = new ConcurrentLinkedQueue<>();

    /** A map of the current test servers, by endpoint name */
    final ConcurrentMap<String, TestServer> testServerMap = new ConcurrentHashMap<>();

    /** A map of maps to endpoint->routers, mapped for each runtime instance captured */
    final ConcurrentMap<CorfuRuntime, Map<String, TestClientRouter>>
            runtimeRouterMap = new ConcurrentHashMap<>();

    /** Test Endpoint hostname. */
    private final static String testHostname = "tcp://test";

    static EventLoopGroup NETTY_EVENT_LOOP;

    /** Initialize the AbstractViewTest. */
    public AbstractViewTest() {
        MockitoAnnotations.openMocks(this);
        // Force all new CorfuRuntimes to override the getRouterFn
        CorfuRuntime.overrideGetRouterFunction = this::getRouterFunction;
        runtime = CorfuRuntime.fromParameters(CorfuRuntimeParameters.builder()
                .nettyEventLoop(NETTY_EVENT_LOOP)
                .maxMvoCacheEntries(MVO_CACHE_SIZE)
                .build());
        // Default number of times to read before hole filling to 0
        // (most aggressive, to surface concurrency issues).
        runtime.getParameters().setHoleFillRetry(0);
    }

    public CorfuRuntime getNewRuntime(@Nonnull NodeLocator node) {
        CorfuRuntime runtime = getNewRuntime(CorfuRuntimeParameters
                .builder()
                .maxMvoCacheEntries(MVO_CACHE_SIZE)
                .build());
        runtime.parseConfigurationString(node.toEndpointUrl());
        return runtime;
    }

    public CorfuRuntime getNewRuntime(@Nonnull CorfuRuntimeParameters parameters) {
        parameters.setNettyEventLoop(NETTY_EVENT_LOOP);
        CorfuRuntime rt = CorfuRuntime.fromParameters(parameters);
        runtimes.add(rt);
        return rt;
    }

    public NodeLocator getDefaultNode() {
        return NodeLocator.builder()
            .host("test")
            .port(SERVERS.PORT_0)
            .nodeId(getServer(SERVERS.PORT_0).serverContext.getNodeId())
            .build();
    }

    public void simulateEndpointDisconnected(CorfuRuntime runtime) {
        ((TestClientRouter) runtime.getRouter(getDefaultEndpoint()))
                .simulateDisconnectedEndpoint();
    }

    /**
     * A helper method that creates a basic message header populated
     * with default values.
     *
     * @param ignoreClusterId   indicates if the message is clusterId aware
     * @param ignoreEpoch       indicates if the message is epoch aware
     * @return                  the corresponding HeaderMsg
     */
    private HeaderMsg getBasicHeader(ClusterIdCheck ignoreClusterId, EpochCheck ignoreEpoch) {
        return getHeaderMsg(1L, CorfuMessage.PriorityLevel.NORMAL, 0L,
                getUuidMsg(DEFAULT_UUID), getUuidMsg(DEFAULT_UUID), ignoreClusterId, ignoreEpoch);
    }

    /**
     * Function for obtaining a router, given a runtime and an endpoint.
     *
     * @param runtime  The CorfuRuntime to obtain a router for.
     * @param endpoint An endpoint string for the router.
     * @return client router
     */
    protected IClientRouter getRouterFunction(CorfuRuntime runtime, String endpoint) {
        runtimeRouterMap.putIfAbsent(runtime, new ConcurrentHashMap<>());
        if (!endpoint.startsWith("test:") && !endpoint.startsWith(testHostname)) {
            throw new RuntimeException("Unsupported endpoint in test: " + endpoint);
        }
        return runtimeRouterMap.get(runtime).computeIfAbsent(endpoint, x -> {
                    String serverName = endpoint.startsWith(testHostname) ?
                            endpoint.substring(endpoint.indexOf("test"), endpoint.length() - 1)
                            : endpoint;
                    TestServer testServer = testServerMap.get(serverName);
                    TestServerRouter serverRouter = testServer.getServerRouter();
                    TestClientRouter tcn = new TestClientRouter(serverRouter);
                    tcn.addClient(new BaseHandler())
                            .addClient(new SequencerHandler())
                            .addClient(new LayoutHandler())
                            .addClient(new LogUnitHandler())
                            .addClient(new ManagementHandler());
                    return tcn;
                }
        );
    }

    /**
     * Before each test, reset the tests.
     */
    @Before
    public void resetTests() {
        runtime.parseConfigurationString(getDefaultConfigurationString());
        runtime.getAddressSpaceView().resetCaches();
    }

    public void resetTests(CorfuRuntimeParameters corfuRuntimeParameters) {
        runtime.shutdown();

        runtime = CorfuRuntime
                .fromParameters(corfuRuntimeParameters)
                .parseConfigurationString(getDefaultConfigurationString());

        runtime.getAddressSpaceView().resetCaches();
    }

    @After
    public void cleanupBuffers() {
        testServerMap.values().forEach(server -> {
            server.getLogUnitServer().shutdown();
            server.getManagementServer().shutdown();
            server.getLayoutServer().shutdown();
            server.getSequencerServer().shutdown();
            server.getBaseServer().shutdown();
        });
        // Abort any active transactions...
        while (runtime.getObjectsView().TXActive()) {
            runtime.getObjectsView().TXAbort();
        }

        runtimeRouterMap.clear();
        testServerMap.clear();

        while (!runtimes.isEmpty()) {
            CorfuRuntime rt = runtimes.poll();
            try {
                rt.shutdown();
            } catch (Throwable th) {
                log.warn("Error closing a runtime", th);
            }
        }
    }

    public void shutdownServer(int port) {
        getServer(port).getLogUnitServer().shutdown();
        getServer(port).getManagementServer().shutdown();
        getServer(port).getLayoutServer().shutdown();
        getServer(port).getSequencerServer().shutdown();
        getServer(port).getBaseServer().shutdown();
    }

    /**
     * Add a server to a specific port, using the given ServerContext.
     * @param port port
     * @param serverContext server context
     */
    public void addServer(int port, ServerContext serverContext) {
        new TestServer(serverContext).addToTest(port, this);
    }

    /** Add a default, in-memory unbootstrapped server at a specific port.
     *
     * @param port      The port to use.
     */
    public void addServer(int port) {
        new TestServer(new ServerContextBuilder().setSingle(false)
            .setServerRouter(new TestServerRouter(port))
            .setPort(port).build())
            .addToTest(port, this);
    }


    /** Add a default, in-memory bootstrapped single node server at a specific port.
     *
     * @param port      The port to use.
     */
    public void addSingleServer(int port) {
        TestServer testServer = new TestServer(port);
        testServer.addToTest(port, this);
        Layout layout = testServer.getLayoutServer().getCurrentLayout();
        bootstrapAllServers(layout);
    }


    /** Get a instance of a test server, which provides access to the underlying components and server router.
     *
     * @param port      The port of the test server to retrieve.
     * @return          A test server instance.
     */
    public TestServer getServer(int port) {
        return testServerMap.get("test:" + port);
    }

    /** Get a instance of a logging unit, given a port.
     *
     * @param port      The port of the logging unit to retrieve.
     * @return          A logging unit instance.
     */
    public LogUnitServer getLogUnit(int port) {
        return getServer(port).getLogUnitServer();
    }

    /** Get a instance of a sequencer, given a port.
     *
     * @param port      The port of the sequencer to retrieve.
     * @return          A sequencer instance.
     */
    public SequencerServer getSequencer(int port) {
        return getServer(port).getSequencerServer();
    }

    /** Get a instance of a layout server, given a port.
     *
     * @param port      The port of the layout server to retrieve.
     * @return          A layout server instance.
     */
    public LayoutServer getLayoutServer(int port) {
        return getServer(port).getLayoutServer();
    }

    /**
     * Get an instance of the management server, given a port
     *
     * @param port The port of the management server to retrieve
     * @return A management server instance.
     */
    public ManagementServer getManagementServer(int port) {
        return getServer(port).getManagementServer();
    }

    public IServerRouter getServerRouter(int port) {
        return getServer(port).getServerRouter();
    }

    /** Bootstraps all servers with a particular layout.
     *
     * @param l         The layout to bootstrap all servers with.
     */
    public void bootstrapAllServers(Layout l) {
        testServerMap.entrySet().parallelStream()
                .forEach(e -> {
                    e.getValue().layoutServer.handleMessage(getRequestMsg(
                                    getBasicHeader(ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                                    getBootstrapLayoutRequestMsg(l)), null, e.getValue().serverRouter);
                    e.getValue().managementServer.handleMessage(getRequestMsg(
                                    getBasicHeader(ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                                    getBootstrapManagementRequestMsg(l)), null, e.getValue().serverRouter);
                });
        TestServer primarySequencerNode = testServerMap.get(l.getSequencers().get(0));

        RequestMsg requestMsg = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getBootstrapSequencerRequestMsg(
                        Collections.emptyMap(),
                        0L,
                        l.getEpoch(),
                        false
                )
        );
        primarySequencerNode.sequencerServer
                .handleMessage(requestMsg, null, primarySequencerNode.serverRouter);
    }


    /**
     * Set aggressive timeouts for the detectors.
     *
     * @param managementServersPorts Management server endpoints.
     */
    void setAggressiveDetectorTimeouts(int... managementServersPorts) {
        Arrays.stream(managementServersPorts).forEach(port -> {
            final int retries = 3;
            FailureDetector.PollConfig config = new FailureDetector.PollConfig(retries, PARAMETERS.TIMEOUT_SHORT);
            FailureDetector failureDetector = getManagementServer(port)
                    .getManagementAgent()
                    .getRemoteMonitoringService()
                    .getFailureDetector();
            failureDetector.setPollConfig(config);
        });
    }

    /**
     * Increment the cluster layout epoch by 1.
     *
     * @return New committed layout
     * @throws OutrankedException If layout proposal is outranked.
     */
    Layout incrementClusterEpoch(CorfuRuntime corfuRuntime) throws OutrankedException {
        corfuRuntime.invalidateLayout();
        Layout layout = new Layout(corfuRuntime.getLayoutView().getLayout());
        layout.setEpoch(layout.getEpoch() + 1);
        corfuRuntime.getLayoutView().getRuntimeLayout(layout).sealMinServerSet();
        corfuRuntime.getLayoutView().updateLayout(layout, 1L);
        return layout;
    }

    /** Get a default CorfuRuntime. The default CorfuRuntime is connected to a single-node
     * in-memory server at port 9000.
     * @return  A default CorfuRuntime
     */
    public CorfuRuntime getDefaultRuntime() {
        if (!testServerMap.containsKey(getEndpoint(SERVERS.PORT_0))) {
            addSingleServer(SERVERS.PORT_0);
        }
        return getRuntime().connect();
    }

    @BeforeClass
    public static void initEventGroup() {
        NETTY_EVENT_LOOP =
            new DefaultEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2,
                new ThreadFactoryBuilder()
                    .setNameFormat("netty-%d")
                    .setDaemon(true)
                    .setUncaughtExceptionHandler((thread, throwable) -> {
                        assertThat(false)
                            .as("Thread " + thread.getName()
                                + " unexpectedly terminated with "
                                + throwable.getClass().getSimpleName())
                            .isTrue();
                    })
                .build());
    }

    @AfterClass
    public static void cleanEventGroup() {
        NETTY_EVENT_LOOP
                .shutdownGracefully(QUIET_PERIOD, TIMEOUT, TimeUnit.MILLISECONDS)
                .syncUninterruptibly();
    }

    /**
     * Create a runtime based on the provided layout.
     * @param l layout
     * @return corfu runtime
     */
    public CorfuRuntime getRuntime(Layout l) {
        CorfuRuntimeParameters parameters = CorfuRuntimeParameters.builder()
            .nettyEventLoop(NETTY_EVENT_LOOP)
            .build();
        CorfuRuntime runtime = CorfuRuntime.fromParameters(parameters);
        runtime.parseConfigurationString(String.join(",", l.layoutServers));
        return runtime;
    }


    /** Clear installed rules for a given runtime.
     *
     * @param r     The runtime to clear rules for.
     */
    public void clearClientRules(CorfuRuntime r) {
        runtimeRouterMap.get(r).values().forEach(x -> x.rules.clear());
    }

    /** Add a rule for a particular runtime.
     *
     * @param r     The runtime to install the rule to
     * @param rule  The rule to install.
     */
    public void addClientRule(CorfuRuntime r, TestRule rule) {
        runtimeRouterMap.get(r).values().forEach(x -> x.rules.add(rule));
    }

    /** Add a rule to a particular router in a particular runtime.
     *
     * @param r                     The runtime to install the rule to
     * @param clientRouterEndpoint  The Client router endpoint to install the rule to
     * @param rule                  The rule to install.
     */
    public void addClientRule(CorfuRuntime r, String clientRouterEndpoint, TestRule rule) {
        runtimeRouterMap.get(r).get(clientRouterEndpoint).rules.add(rule);
    }

    /** Clear rules for a particular server.
     *
     * @param port  The port of the server to clear rules for.
     */
    public void clearServerRules(int port) {
        getServer(port).getServerRouter().rules.clear();
    }

    /** Install a rule to a particular server.
     *
     * @param port  The port of the server to install the rule to.
     * @param rule  The rule to install.
     */
    public void addServerRule(int port, TestRule rule) {
        getServer(port).getServerRouter().rules.add(rule);
    }

    /** The configuration string used for the default runtime.
     *
     * @return  The configuration string used for the default runtime.
     */
    public String getDefaultConfigurationString() {
        return getDefaultEndpoint();
    }

    /** The default endpoint (single server) used for the default runtime.
     *
     * @return  Returns the default endpoint.
     */
    public String getDefaultEndpoint() {
        return getEndpoint(SERVERS.PORT_0);
    }

    /** Get the endpoint string, given a port number.
     *
     * @param port  The port number to get an endpoint string for.
     * @return      The endpoint string.
     */
    public String getEndpoint(int port) {
        return "test:" + port;
    }

    /**
     * This class holds instances of servers used for test.
     */
    @Data
    private static class TestServer {
        ServerContext serverContext;
        BaseServer baseServer;
        SequencerServer sequencerServer;
        LayoutServer layoutServer;
        LogUnitServer logUnitServer;
        ManagementServer managementServer;
        IServerRouter serverRouter;
        int port;

        TestServer(Map<String, Object> optsMap) {
            this(new ServerContext(optsMap));
            serverContext.setServerRouter(new TestServerRouter());
        }

        TestServer(ServerContext serverContext) {
            this.serverContext = serverContext;
            this.serverRouter = serverContext.getServerRouter();
            if (serverRouter == null) {
                serverRouter = new TestServerRouter();
            }
            this.baseServer = new BaseServer(serverContext);
            this.sequencerServer = new SequencerServer(serverContext);
            this.layoutServer = new LayoutServer(serverContext);
            this.logUnitServer = new LogUnitServer(serverContext);
            this.managementServer = new ManagementServer(serverContext,
                    new ManagementServer.ManagementServerInitializer());

            this.serverRouter.addServer(baseServer);
            this.serverRouter.addServer(sequencerServer);
            this.serverRouter.addServer(layoutServer);
            this.serverRouter.addServer(logUnitServer);
            this.serverRouter.addServer(managementServer);
            serverContext.setServerRouter(this.serverRouter);
            this.serverRouter.setServerContext(this.serverContext);
            if(serverContext.isSingleNodeSetup() && serverContext.getCurrentLayout() != null){
                serverContext.setServerEpoch(serverContext.getCurrentLayout().getEpoch(),
                        this.serverRouter);
            }
        }

        TestServer(int port) {
            this(ServerContextBuilder.defaultTestContext(port).getServerConfig());
        }

        void addToTest(int port, AbstractViewTest test) {

            if (test.testServerMap.putIfAbsent("test:" + port, this) != null) {
                throw new RuntimeException("Server already registered at port " + port);
            }

        }

        public TestServerRouter getServerRouter() {
            return (TestServerRouter) this.serverRouter;
        }
    }
}

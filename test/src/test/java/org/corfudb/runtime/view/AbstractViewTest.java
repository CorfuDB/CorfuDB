package org.corfudb.runtime.view;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Data;
import lombok.Getter;
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
import org.corfudb.infrastructure.management.NetworkStretcher;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.LayoutBootstrapRequest;
import org.corfudb.protocols.wireprotocol.SequencerRecoveryMsg;
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
import org.corfudb.util.NodeLocator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * This class serves as a base class for most higher-level Corfu unit tests providing several helper
 * functions to reduce boilerplate code.
 *
 * <p>For most tests, a CorfuRuntime can be obtained by calling getDefaultRuntime(). This
 * instantiates a single-node in-memory Corfu server at port 9000, already bootstrapped. If
 * getDefaultRuntime() is not called, then no servers are started.
 *
 * <p>For all other tests, servers can be started using the addServer(port, options) function. The
 * bootstrapAllServers(layout) function can be used to bootstrap the servers with a specific layout.
 * These servers can be referred to by a CorfuRuntime using the "test:<port number>" convention. For
 * example, calling new CorfuRuntime("test:9000"); will connect a CorfuRuntime to the test server at
 * port 9000.
 *
 * <p>To access servers, call the getLogUnit(port), getLayoutServer(port) and getSequencer(port).
 * This allows access to the server class public fields and methods.
 *
 * <p>In addition to simulating Corfu servers, this class also permits installing special rules,
 * which can be used to simulate failures or reorder messages. To install, use the
 * addClientRule(testRule) and addServerRule(testRule) methods.
 *
 * <p>Created by mwei on 12/22/15.
 */
public abstract class AbstractViewTest extends AbstractCorfuTest {
  private static final int QUIET_PERIOD = 100;
  private static final int TIMEOUT = 300;

  /** The runtime generated by default, by getDefaultRuntime(). */
  @Getter CorfuRuntime runtime;

  /** A map of the current test servers, by endpoint name */
  final Map<String, TestServer> testServerMap = new ConcurrentHashMap<>();

  /** A map of maps to endpoint->routers, mapped for each runtime instance captured */
  final Map<CorfuRuntime, Map<String, TestClientRouter>> runtimeRouterMap =
      new ConcurrentHashMap<>();

  /** Test Endpoint hostname. */
  private static final String testHostname = "tcp://test";

  /** Initialize the AbstractViewTest. */
  public AbstractViewTest() {
    this(false);
  }

  public AbstractViewTest(boolean followBackpointers) {
    // Force all new CorfuRuntimes to override the getRouterFn
    CorfuRuntime.overrideGetRouterFunction = this::getRouterFunction;
    runtime =
        CorfuRuntime.fromParameters(
            CorfuRuntimeParameters.builder()
                .followBackpointersEnabled(followBackpointers)
                .nettyEventLoop(NETTY_EVENT_LOOP)
                .build());
    // Default number of times to read before hole filling to 0
    // (most aggressive, to surface concurrency issues).
    runtime.getParameters().setHoleFillRetry(0);
  }

  public CorfuRuntime getNewRuntime(@Nonnull NodeLocator node) {
    CorfuRuntime runtime = getNewRuntime(CorfuRuntimeParameters.builder().build());
    runtime.parseConfigurationString(node.getHost() + ":" + node.getPort());
    return runtime;
  }

  public CorfuRuntime getNewRuntime(@Nonnull CorfuRuntimeParameters parameters) {
    parameters.setNettyEventLoop(NETTY_EVENT_LOOP);
    return CorfuRuntime.fromParameters(parameters);
  }

  public NodeLocator getDefaultNode() {
    return NodeLocator.builder()
        .host("test")
        .port(SERVERS.PORT_0)
        .nodeId(getServer(SERVERS.PORT_0).serverContext.getNodeId())
        .build();
  }

  public void simulateEndpointDisconnected(CorfuRuntime runtime) {
    ((TestClientRouter) runtime.getRouter(getDefaultEndpoint())).simulateDisconnectedEndpoint();
  }

  /**
   * Function for obtaining a router, given a runtime and an endpoint.
   *
   * @param runtime The CorfuRuntime to obtain a router for.
   * @param endpoint An endpoint string for the router.
   * @return
   */
  protected IClientRouter getRouterFunction(CorfuRuntime runtime, String endpoint) {
    runtimeRouterMap.putIfAbsent(runtime, new ConcurrentHashMap<>());
    if (!endpoint.startsWith("test:") && !endpoint.startsWith(testHostname)) {
      throw new RuntimeException("Unsupported endpoint in test: " + endpoint);
    }
    return runtimeRouterMap
        .get(runtime)
        .computeIfAbsent(
            endpoint,
            x -> {
              String serverName =
                  endpoint.startsWith(testHostname)
                      ? endpoint.substring(endpoint.indexOf("test"), endpoint.length() - 1)
                      : endpoint;
              TestClientRouter tcn =
                  new TestClientRouter(testServerMap.get(serverName).getServerRouter());
              tcn.addClient(new BaseHandler())
                  .addClient(new SequencerHandler())
                  .addClient(new LayoutHandler())
                  .addClient(new LogUnitHandler())
                  .addClient(new ManagementHandler());
              return tcn;
            });
  }

  /** Before each test, reset the tests. */
  @Before
  public void resetTests() {
    runtime.parseConfigurationString(getDefaultConfigurationString());
    //         .setCacheDisabled(true); // Disable cache during unit tests to fully stress the
    // system.
    runtime.getAddressSpaceView().resetCaches();
  }

  @After
  public void cleanupBuffers() {
    testServerMap
        .values()
        .forEach(
            server -> {
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

    runtimeRouterMap.keySet().forEach(CorfuRuntime::shutdown);
    runtimeRouterMap.clear();
    testServerMap.clear();
    runtime.shutdown();
  }

  /**
   * Add a server to a specific port, using the given ServerContext.
   *
   * @param port
   * @param serverContext
   */
  public void addServer(int port, ServerContext serverContext) {
    new TestServer(serverContext).addToTest(port, this);
  }

  /**
   * Add a default, in-memory unbootstrapped server at a specific port.
   *
   * @param port The port to use.
   */
  public void addServer(int port) {
    new TestServer(
            new ServerContextBuilder()
                .setSingle(false)
                .setServerRouter(new TestServerRouter(port))
                .setPort(port)
                .build())
        .addToTest(port, this);
  }

  /**
   * Add a default, in-memory bootstrapped single node server at a specific port.
   *
   * @param port The port to use.
   */
  public void addSingleServer(int port) {
    TestServer testServer = new TestServer(port);
    testServer.addToTest(port, this);
    Layout layout = testServer.getLayoutServer().getCurrentLayout();
    bootstrapAllServers(layout);
  }

  /**
   * Get a instance of a test server, which provides access to the underlying components and server
   * router.
   *
   * @param port The port of the test server to retrieve.
   * @return A test server instance.
   */
  public TestServer getServer(int port) {
    return testServerMap.get("test:" + port);
  }

  /**
   * Get a instance of a logging unit, given a port.
   *
   * @param port The port of the logging unit to retrieve.
   * @return A logging unit instance.
   */
  public LogUnitServer getLogUnit(int port) {
    return getServer(port).getLogUnitServer();
  }

  /**
   * Get a instance of a sequencer, given a port.
   *
   * @param port The port of the sequencer to retrieve.
   * @return A sequencer instance.
   */
  public SequencerServer getSequencer(int port) {
    return getServer(port).getSequencerServer();
  }

  /**
   * Get a instance of a layout server, given a port.
   *
   * @param port The port of the layout server to retrieve.
   * @return A layout server instance.
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

  /**
   * Bootstraps all servers with a particular layout.
   *
   * @param l The layout to bootstrap all servers with.
   */
  public void bootstrapAllServers(Layout l) {
    testServerMap.entrySet().parallelStream()
        .forEach(
            e -> {
              e.getValue()
                  .layoutServer
                  .handleMessage(
                      CorfuMsgType.LAYOUT_BOOTSTRAP.payloadMsg(new LayoutBootstrapRequest(l)),
                      null,
                      e.getValue().serverRouter);
              e.getValue()
                  .managementServer
                  .handleMessage(
                      CorfuMsgType.MANAGEMENT_BOOTSTRAP_REQUEST.payloadMsg(l),
                      null,
                      e.getValue().serverRouter);
            });
    TestServer primarySequencerNode = testServerMap.get(l.getSequencers().get(0));
    primarySequencerNode.sequencerServer.handleMessage(
        CorfuMsgType.BOOTSTRAP_SEQUENCER.payloadMsg(
            new SequencerRecoveryMsg(0L, Collections.emptyMap(), l.getEpoch(), false)),
        null,
        primarySequencerNode.serverRouter);
  }

  /**
   * Set aggressive timeouts for the detectors.
   *
   * @param managementServersPorts Management server endpoints.
   */
  void setAggressiveDetectorTimeouts(int... managementServersPorts) {
    Arrays.stream(managementServersPorts)
        .forEach(
            port -> {
              NetworkStretcher stretcher =
                  NetworkStretcher.builder()
                      .periodDelta(PARAMETERS.TIMEOUT_VERY_SHORT)
                      .maxPeriod(PARAMETERS.TIMEOUT_VERY_SHORT)
                      .initialPollInterval(PARAMETERS.TIMEOUT_VERY_SHORT)
                      .build();

              FailureDetector failureDetector =
                  getManagementServer(port)
                      .getManagementAgent()
                      .getRemoteMonitoringService()
                      .getFailureDetector();
              failureDetector.setNetworkStretcher(stretcher);
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

  /**
   * Get a default CorfuRuntime. The default CorfuRuntime is connected to a single-node in-memory
   * server at port 9000.
   *
   * @return A default CorfuRuntime
   */
  public CorfuRuntime getDefaultRuntime() {
    if (!testServerMap.containsKey(getEndpoint(SERVERS.PORT_0))) {
      addSingleServer(SERVERS.PORT_0);
    }
    return getRuntime().connect();
  }

  static EventLoopGroup NETTY_EVENT_LOOP;

  @BeforeClass
  public static void initEventGroup() {
    NETTY_EVENT_LOOP =
        new DefaultEventLoopGroup(
            Runtime.getRuntime().availableProcessors() * 2,
            new ThreadFactoryBuilder()
                .setNameFormat("netty-%d")
                .setDaemon(true)
                .setUncaughtExceptionHandler(
                    (thread, throwable) -> {
                      assertThat(false)
                          .as(
                              "Thread "
                                  + thread.getName()
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
   *
   * @param l
   * @return
   */
  public CorfuRuntime getRuntime(Layout l) {
    CorfuRuntimeParameters parameters =
        CorfuRuntimeParameters.builder().nettyEventLoop(NETTY_EVENT_LOOP).build();
    CorfuRuntime runtime = CorfuRuntime.fromParameters(parameters);
    runtime.parseConfigurationString(l.layoutServers.stream().collect(Collectors.joining(",")));
    return runtime;
  }

  /**
   * Clear installed rules for a given runtime.
   *
   * @param r The runtime to clear rules for.
   */
  public void clearClientRules(CorfuRuntime r) {
    runtimeRouterMap.get(r).values().forEach(x -> x.rules.clear());
  }

  /**
   * Add a rule for the default runtime.
   *
   * @param rule The rule to install
   */
  public void addClientRule(TestRule rule) {
    addClientRule(getRuntime(), rule);
  }

  /**
   * Add a rule for a particular runtime.
   *
   * @param r The runtime to install the rule to
   * @param rule The rule to install.
   */
  public void addClientRule(CorfuRuntime r, TestRule rule) {
    runtimeRouterMap.get(r).values().forEach(x -> x.rules.add(rule));
  }

  /**
   * Add a rule to a particular router in a particular runtime.
   *
   * @param r The runtime to install the rule to
   * @param clientRouterEndpoint The Client router endpoint to install the rule to
   * @param rule The rule to install.
   */
  public void addClientRule(CorfuRuntime r, String clientRouterEndpoint, TestRule rule) {
    runtimeRouterMap.get(r).get(clientRouterEndpoint).rules.add(rule);
  }

  /**
   * Clear rules for a particular server.
   *
   * @param port The port of the server to clear rules for.
   */
  public void clearServerRules(int port) {
    getServer(port).getServerRouter().rules.clear();
  }

  /**
   * Install a rule to a particular server.
   *
   * @param port The port of the server to install the rule to.
   * @param rule The rule to install.
   */
  public void addServerRule(int port, TestRule rule) {
    getServer(port).getServerRouter().rules.add(rule);
  }

  /**
   * The configuration string used for the default runtime.
   *
   * @return The configuration string used for the default runtime.
   */
  public String getDefaultConfigurationString() {
    return getDefaultEndpoint();
  }

  /**
   * The default endpoint (single server) used for the default runtime.
   *
   * @return Returns the default endpoint.
   */
  public String getDefaultEndpoint() {
    return getEndpoint(SERVERS.PORT_0);
  }

  /**
   * Get the endpoint string, given a port number.
   *
   * @param port The port number to get an endpoint string for.
   * @return The endpoint string.
   */
  public String getEndpoint(int port) {
    return "test:" + port;
  }

  // Private

  /** This class holds instances of servers used for test. */
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
      this.managementServer = new ManagementServer(serverContext);

      this.serverRouter.addServer(baseServer);
      this.serverRouter.addServer(sequencerServer);
      this.serverRouter.addServer(layoutServer);
      this.serverRouter.addServer(logUnitServer);
      this.serverRouter.addServer(managementServer);
      serverContext.setServerRouter(this.serverRouter);
      this.serverRouter.setServerContext(this.serverContext);
      if (serverContext.isSingleNodeSetup() && serverContext.getCurrentLayout() != null) {
        serverContext.setServerEpoch(
            serverContext.getCurrentLayout().getEpoch(), this.serverRouter);
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

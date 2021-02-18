package org.corfudb.infrastructure;

import lombok.Getter;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseHandler;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutHandler;
import org.corfudb.runtime.clients.LogUnitHandler;
import org.corfudb.runtime.clients.ManagementHandler;
import org.corfudb.runtime.clients.SequencerHandler;
import org.corfudb.runtime.clients.TestClientRouter;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.view.Layout;
import org.junit.Before;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;

/**
 * Created by mwei on 12/12/15.
 */
public abstract class AbstractServerTest extends AbstractCorfuTest {

    public static final UUID testClientId = UUID.nameUUIDFromBytes("TEST_CLIENT".getBytes());

    @Getter
    TestServerRouter router;

    @Getter
    TestClientRouter clientRouter;

    AtomicInteger requestCounter;

    public AbstractServerTest() {
        router = new TestServerRouter();
        requestCounter = new AtomicInteger();
        // Force all new CorfuRuntimes to override the getRouterFn
        CorfuRuntime.overrideGetRouterFunction = this::getRouterFunction;
        clientRouter = getClientRouter();
    }

    public void setServer(AbstractServer server) {
        router.reset();
        router.addServer(server);
    }

    public void setContext(ServerContext sc) {
        router.setServerContext(sc);
    }

    public abstract AbstractServer getDefaultServer();

    public <T> CompletableFuture<T> sendRequest(RequestPayloadMsg payload,
                                                ClusterIdCheck ignoreClusterId, EpochCheck ignoreEpoch) {
        return sendRequestWithClusterId(payload, Layout.INVALID_CLUSTER_ID, ignoreClusterId, ignoreEpoch);
    }

    public <T> CompletableFuture<T> sendRequestWithEpoch(RequestPayloadMsg payload, long epoch,
                                                         ClusterIdCheck ignoreClusterId, EpochCheck ignoreEpoch) {
        clientRouter.setClientID(testClientId);
        return clientRouter.sendRequestAndGetCompletable(payload, epoch, getUuidMsg(Layout.INVALID_CLUSTER_ID),
                CorfuMessage.PriorityLevel.NORMAL, ignoreClusterId, ignoreEpoch);
    }

    public <T> CompletableFuture<T> sendRequestWithClusterId(RequestPayloadMsg payload, UUID clusterId,
                                                             ClusterIdCheck ignoreClusterId, EpochCheck ignoreEpoch) {
        clientRouter.setClientID(testClientId);
        return clientRouter.sendRequestAndGetCompletable(payload, 0L, getUuidMsg(clusterId),
                CorfuMessage.PriorityLevel.NORMAL, ignoreClusterId, ignoreEpoch);
    }

    public <T> CompletableFuture<T> sendRequestWithClientId(UUID clientId, RequestPayloadMsg payload, UUID clusterId,
                                                            ClusterIdCheck ignoreClusterId, EpochCheck ignoreEpoch) {
        clientRouter.setClientID(clientId);
        return clientRouter.sendRequestAndGetCompletable(payload, 0L, getUuidMsg(clusterId),
                CorfuMessage.PriorityLevel.NORMAL, ignoreClusterId, ignoreEpoch);
    }


    public TestClientRouter getClientRouter() {
        TestClientRouter tcn = new TestClientRouter(router);
        tcn.setClientID(testClientId);
        tcn.addClient(new BaseHandler())
                .addClient(new SequencerHandler())
                .addClient(new LayoutHandler())
                .addClient(new LogUnitHandler())
                .addClient(new ManagementHandler());
        return tcn;
    }

    @Before
    public void resetTest() {
        router.reset();
        router.addServer(getDefaultServer());
        requestCounter.set(0);
    }

    /**
     * A map of maps to endpoint->routers, mapped for each runtime instance captured
     */
    final Map<CorfuRuntime, Map<String, TestClientRouter>>
            runtimeRouterMap = new ConcurrentHashMap<>();

    /**
     * Function for obtaining a router, given a runtime and an endpoint.
     *
     * @param runtime  The CorfuRuntime to obtain a router for.
     * @param endpoint An endpoint string for the router.
     * @return
     */
    private IClientRouter getRouterFunction(CorfuRuntime runtime, String endpoint) {
        runtimeRouterMap.putIfAbsent(runtime, new ConcurrentHashMap<>());
        if (!endpoint.startsWith("test:")) {
            throw new RuntimeException("Unsupported endpoint in test: " + endpoint);
        }
        return runtimeRouterMap.get(runtime).computeIfAbsent(endpoint,
                x -> {
                    TestClientRouter tcn =
                            new TestClientRouter(router);
                    tcn.addClient(new BaseHandler())
                            .addClient(new SequencerHandler())
                            .addClient(new LayoutHandler())
                            .addClient(new LogUnitHandler())
                            .addClient(new ManagementHandler());
                    return tcn;
                }
        );
    }
}

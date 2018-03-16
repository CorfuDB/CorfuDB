package org.corfudb.infrastructure;

import lombok.Getter;
import org.assertj.core.api.Assertions;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.*;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by mwei on 12/12/15.
 */
public abstract class AbstractServerTest extends AbstractCorfuTest {

    public static final UUID testClientId = UUID.nameUUIDFromBytes("TEST_CLIENT".getBytes());

    @Getter
    TestServerRouter router;

    AtomicInteger requestCounter;

    public AbstractServerTest() {
        router = new TestServerRouter();
        requestCounter = new AtomicInteger();
        // Force all new CorfuRuntimes to override the getRouterFn
        CorfuRuntime.overrideGetRouterFunction = this::getRouterFunction;
    }

    public void setServer(AbstractServer server) {
        router.reset();
        router.addServer(server);
    }

    public abstract AbstractServer getDefaultServer();

    @Before
    public void resetTest() {
        router.reset();
        router.addServer(getDefaultServer());
        requestCounter.set(0);
    }

    public List<CorfuMsg> getResponseMessages() {
        return router.getResponseMessages();
    }

    public CorfuMsg getLastMessage() {
        if (router.getResponseMessages().size() == 0) return null;
        return router.getResponseMessages().get(router.getResponseMessages().size() - 1);
    }

    @SuppressWarnings("unchecked")
    public <T extends CorfuMsg> T getLastMessageAs(Class<T> type) {
        return (T) getLastMessage();
    }

    @SuppressWarnings("unchecked")
    public <T> T getLastPayloadMessageAs(Class<T> type) {
        Assertions.assertThat(getLastMessage())
                .isInstanceOf(CorfuPayloadMsg.class);
        return ((CorfuPayloadMsg<T>)getLastMessage()).getPayload();
    }
    public void sendMessage(CorfuMsg message) {
        sendMessage(testClientId, message);
    }

    public void sendMessage(UUID clientId, CorfuMsg message) {
        message.setClientID(clientId);
        message.setRequestID(requestCounter.getAndIncrement());
        router.sendServerMessage(message);
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
                    tcn.addClient(new BaseClient())
                            .addClient(new SequencerClient())
                            .addClient(new LayoutClient())
                            .addClient(new LogUnitClient())
                            .addClient(new ManagementClient());
                    return tcn;
                }
        );
    }
}

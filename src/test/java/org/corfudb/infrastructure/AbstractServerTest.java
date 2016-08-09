package org.corfudb.infrastructure;

import lombok.Getter;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.junit.Before;

import java.util.List;
import java.util.UUID;
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
        router = new TestServerRouter(ServerContextBuilder.emptyContext());
        requestCounter = new AtomicInteger();
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

    public void sendMessage(CorfuMsg message) {
        sendMessage(testClientId, message);
    }

    public void sendMessage(UUID clientId, CorfuMsg message) {
        message.setClientID(clientId);
        message.setRequestID(requestCounter.getAndIncrement());
        router.sendServerMessage(message);
    }
}

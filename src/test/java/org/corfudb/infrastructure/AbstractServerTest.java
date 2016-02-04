package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.junit.Before;

import java.util.List;
import java.util.Map;

/**
 * Created by mwei on 12/12/15.
 */
public abstract class AbstractServerTest extends AbstractCorfuTest {

    TestServerRouter router;

    public AbstractServerTest() {
        router = new TestServerRouter();
    }

    public void setServer(IServer server)
    {
        router.setServerUnderTest(server);
    }

    public abstract IServer getDefaultServer();

    @Before
    public void resetTest()
    {
        router.reset();
        router.setServerUnderTest(getDefaultServer());
    }

    public List<CorfuMsg> getResponseMessages()
    {
        return router.getResponseMessages();
    }

    public CorfuMsg getLastMessage()
    {
        return router.getResponseMessages().get(router.getResponseMessages().size()-1);
    }

    @SuppressWarnings("unchecked")
    public <T extends CorfuMsg> T getLastMessageAs(Class<T> type)
    {
        return (T) getLastMessage();
    }

    public void sendMessage(CorfuMsg message)
    {
        router.sendServerMessage(message);
    }

    public Map<String,Object> defaultOptionsMap()
    {
        return new ImmutableMap.Builder<String,Object>()
                        .put("--initial-token", "0")
                        .put("--single", false)
                        .put("--memory", true)
                        .build();
    }
}

package org.corfudb.infrastructure;

import org.corfudb.runtime.protocols.BaseNettyClient;
import org.corfudb.runtime.protocols.LayoutClient;
import org.corfudb.runtime.protocols.NettyClientRouter;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 12/8/15.
 */
public class NettyRouterTest {

    NettyClientRouter ncr;

    @Before
    public void setupTest() {
        ncr = new NettyClientRouter("localhost", 9999);
        ncr.addClient(new LayoutClient())
                .start();
    }

    @Test
    public void pingTest()
    {
        System.out.println("ping1");
        assertThat(ncr.getClient(BaseNettyClient.class).pingSync())
                .isTrue();
        System.out.println("ping2");
    }

    @Test
    public void layoutTest()
            throws Exception
    {
        System.out.println(ncr.getClient(LayoutClient.class).getLayout().get());
    }
}

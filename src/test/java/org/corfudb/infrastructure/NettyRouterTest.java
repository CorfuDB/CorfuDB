package org.corfudb.infrastructure;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.corfudb.infrastructure.wireprotocol.NettyCorfuMessageDecoder;
import org.corfudb.infrastructure.wireprotocol.NettyCorfuMessageEncoder;
import org.corfudb.infrastructure.wireprotocol.NettyCorfuMsg;
import org.corfudb.infrastructure.wireprotocol.NettyStreamingServerTokenResponseMsg;
import org.corfudb.runtime.protocols.BaseNettyClient;
import org.corfudb.runtime.protocols.LayoutClient;
import org.corfudb.runtime.protocols.NettyClientRouter;
import org.corfudb.runtime.protocols.NettyRPCChannelInboundHandlerAdapter;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

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

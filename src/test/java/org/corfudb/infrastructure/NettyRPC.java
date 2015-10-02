package org.corfudb.infrastructure;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.corfudb.infrastructure.wireprotocol.NettyCorfuMsg;
import org.corfudb.runtime.protocols.AbstractNettyProtocol;
import org.corfudb.runtime.protocols.NettyRPCChannelInboundHandlerAdapter;
import org.corfudb.util.SizeBufferPool;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by dmalkhi on 9/30/15.
 */
public class NettyRPC {

    final int nmessages = 3;

    @AllArgsConstructor
    @Getter
    class NettyTestMsg extends NettyCorfuMsg {
        int mid;

        @Override
        public void serialize(ByteBuf buffer) {
            super.serialize(buffer);
            buffer.writeInt(mid);
        }

        @Override
        public void fromBuffer(ByteBuf buffer) {
            super.fromBuffer(buffer);
            mid = buffer.readInt();
        }
    }

    // my Netty test server
    //
    class NettyTestServer extends AbstractNettyServer {

        AtomicInteger nreceived = new AtomicInteger(0);

        @Override
        void parseConfiguration(Map<String, Object> configuration) {
            System.out.println("parse config");
        }

        @Override
        void processMessage(NettyCorfuMsg msg, ChannelHandlerContext ctx) {
            int r = nreceived.getAndIncrement();
            if (nmessages < 5 || r % (nmessages/5) == 0)
                System.out.println("got mesg :" + msg); // r + ">, requestID = " + msg.getRequestID() + ", epoch=" + msg.getEpoch()) ;
            if (r == nmessages-1)
                System.out.println("!!!!!!!!!!finished all " + r + " messages!!!!!!");
        }

        @Override
        public void reset() {

        }

        @Override
        public Thread getThread() {
            return null;
        }
    }

    class NettyTestClientHandler extends NettyRPCChannelInboundHandlerAdapter {

        @Override
        public void handleMessage(NettyCorfuMsg message) {
            System.out.println("client receive response");
        }
    }

    NettyTestClientHandler nettyTestClientHandler = new NettyTestClientHandler();

    // my Netty test client
    //
    class NettyTestClient  extends AbstractNettyProtocol<NettyRPC.NettyTestClientHandler>
    {

        public NettyTestClient() {
            super("localhost", 9999, null, 0, nettyTestClientHandler);
        }

       public void run() {
            // send some requests ...
            System.out.println("send something");
           UUID myid = new UUID(0,0);
           for (int i = 0; i < nmessages; i++) {
               NettyCorfuMsg msg = new NettyCorfuMsg(myid, (long)i, (long) 100, NettyCorfuMsg.NettyCorfuMsgType.ERROR_OK);
               if (nmessages <  5 || i % (nmessages/5) == 0)
                   System.out.println("send msg: " + msg);
               handler.sendMessage(100, msg);
           }
        }

    }

    // ----------------------------------------------------------- //
    public NettyRPC() {
        RPCtest();
    }

    @Test
    public void RPCtest() {
        System.out.println("RPC test starting");
        new Thread(() -> {
            NettyTestServer ns = new NettyTestServer();
            ns.getInstance(new HashMap<String, Object>(){{
                put("port", 9999);
            }});
            ns.run();
        }).start();

 //       new Thread(() -> {
 //           NettyTestClient nc = new NettyTestClient();
 //           nc.run();
 //       }).start();

        try {
            Thread.sleep(5000);
            NettyTestClient nc = new NettyTestClient();
            nc.run();
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        NettyTestClient nc = new NettyTestClient();
        nc.run();

    }
}

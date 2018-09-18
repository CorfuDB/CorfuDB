package org.corfudb.universe.corfuproxy;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.corfudb.runtime.CorfuRuntime;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.corfudb.universe.corfuproxy.Proxy.*;

@Slf4j
@RequiredArgsConstructor
public class CorfuProxyServer {
    @NonNull
    private Integer port;

    private void start() throws Exception {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap sb = new ServerBootstrap();
            sb.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipe = ch.pipeline();
                            // Protobuf decoders
                            pipe.addLast(new ProtobufVarint32FrameDecoder());
                            pipe.addLast(new ProtobufDecoder(ProxyRequestMsg.getDefaultInstance()));
                            // Protobuf encoders
                            pipe.addLast(new ProtobufVarint32LengthFieldPrepender());
                            pipe.addLast(new ProtobufEncoder());
                            // Server Handler
                            pipe.addLast(new CorfuClientProxyHandler());
                        }
                    });

            // Bind and setup to accept incoming connections.
            ChannelFuture future = sb.bind(port).sync();
            // Wait until the server socket is closed.
            future.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    static class CorfuClientProxyHandler extends SimpleChannelInboundHandler<ProxyRequestMsg> {
        private static Map<String, CorfuRuntime> runtimeMap = new ConcurrentHashMap<>();

        private CorfuRuntime runtime;

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ProxyRequestMsg msg) throws Exception {
            runtime = runtimeMap.computeIfAbsent(msg.getClientId(), (key) -> new CorfuRuntime());

            MethodType methodType = msg.getMethodType();
            String methodName = StringUtils.uncapitalize(methodType.getValueDescriptor().getName());
            Method method = CorfuClientProxyHandler.class.getDeclaredMethod(methodName, ProxyRequestMsg.class);
            method.setAccessible(true);
            Object response;

            try {
                 response = method.invoke(this, msg);
            } catch (Exception e) {
                response = buildErrorResponse(e.getCause(), msg);
            }

            ChannelFuture f = ctx.writeAndFlush(response);
            f.addListener(ChannelFutureListener.CLOSE);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("Exception during channel handling.", cause);
            ctx.close();
        }

        private ProxyResponseMsg buildErrorResponse(Throwable t, ProxyRequestMsg msg) {
            StringWriter sw = new StringWriter();
            t.printStackTrace(new PrintWriter(sw));

            return ProxyResponseMsg.newBuilder()
                    .setMessageId(msg.getMessageId())
                    .setErrorMsg(t.toString())
                    .setStackTrace(sw.toString())
                    .build();
        }

        private ProxyResponseMsg connect(ProxyRequestMsg msg) {
            log.info("Received connect() request");

            runtime.connect();

            return ProxyResponseMsg.newBuilder()
                    .setMessageId(msg.getMessageId())
                    .build();
        }

        private ProxyResponseMsg stop(ProxyRequestMsg msg) {
            log.info("Received stop() request");

            if (msg.hasStopRequest() && msg.getStopRequest().hasShutdown()) {
                runtime.stop(msg.getStopRequest().getShutdown());
            } else {
                runtime.stop();
            }

            return ProxyResponseMsg.newBuilder()
                    .setMessageId(msg.getMessageId())
                    .build();
        }

        private ProxyResponseMsg shutdown(ProxyRequestMsg msg) {
            log.info("Received shutdown() request");

            runtime.shutdown();

            return ProxyResponseMsg.newBuilder()
                    .setMessageId(msg.getMessageId())
                    .build();
        }

        private ProxyResponseMsg addNode(ProxyRequestMsg msg) {
            log.info("Received addNode() request");

            if (msg.hasAddNodeRequest()) {
                AddNodeRequest request = msg.getAddNodeRequest();
                runtime.getManagementView().addNode(
                        request.getEndpoint(),
                        request.getRetry(),
                        Duration.parse(request.getTimeout()),
                        Duration.parse(request.getPollPeriod()));
            } else {
                log.error("addNode() request parameters not found");
            }

            return ProxyResponseMsg.newBuilder()
                    .setMessageId(msg.getMessageId())
                    .build();
        }

        private ProxyResponseMsg removeNode(ProxyRequestMsg msg) {
            log.info("Received removeNode() request");

            if (msg.hasRemoveNodeRequest()) {
                RemoveNodeRequest request = msg.getRemoveNodeRequest();
                runtime.getManagementView().removeNode(
                        request.getEndpoint(),
                        request.getRetry(),
                        Duration.parse(request.getTimeout()),
                        Duration.parse(request.getPollPeriod()));
            } else {
                log.error("removeNode() request parameters not found");
            }

            return ProxyResponseMsg.newBuilder()
                    .setMessageId(msg.getMessageId())
                    .build();
        }

        private ProxyResponseMsg forceRemoveNode(ProxyRequestMsg msg) {
            log.info("Received forceRemoveNode() request");

            if (msg.hasForceRemoveNodeRequest()) {
                ForceRemoveNodeRequest request = msg.getForceRemoveNodeRequest();
                runtime.getManagementView().forceRemoveNode(
                        request.getEndpoint(),
                        request.getRetry(),
                        Duration.parse(request.getTimeout()),
                        Duration.parse(request.getPollPeriod()));
            } else {
                log.error("forceRemoveNode() request parameters not found");
            }

            return ProxyResponseMsg.newBuilder()
                    .setMessageId(msg.getMessageId())
                    .build();
        }

        private ProxyResponseMsg addLayoutServer(ProxyRequestMsg msg) {
            log.info("Received addLayoutServer() request");

            if (msg.hasAddLayoutServerRequest()) {
                runtime.addLayoutServer(msg.getAddLayoutServerRequest().getEndpoint());
            } else {
                log.error("addLayoutServer() request parameters not found");
            }

            return ProxyResponseMsg.newBuilder()
                    .setMessageId(msg.getMessageId())
                    .build();
        }

        private ProxyResponseMsg getAllServers(ProxyRequestMsg msg) {
            log.info("Received getAllServers() request");

            Set<String> servers = runtime.getLayoutView().getLayout().getAllServers();
            GetAllServersResponse response = GetAllServersResponse.newBuilder()
                    .addAllServers(servers)
                    .build();

            return ProxyResponseMsg.newBuilder()
                    .setMessageId(msg.getMessageId())
                    .setGetAllServersResponse(response)
                    .build();
        }

        private ProxyResponseMsg getAllActiveServers(ProxyRequestMsg msg) {
            log.info("Received getAllActiveServers() request");

            Set<String> servers = runtime.getLayoutView().getLayout().getAllActiveServers();
            GetAllActiveServersResponse response = GetAllActiveServersResponse.newBuilder()
                    .addAllServers(servers)
                    .build();

            return ProxyResponseMsg.newBuilder()
                    .setMessageId(msg.getMessageId())
                    .setGetAllActiveServersResponse(response)
                    .build();
        }

        private ProxyResponseMsg invalidateLayout(ProxyRequestMsg msg) {
            log.info("Received invalidateLayout() request");

            runtime.invalidateLayout();

            return ProxyResponseMsg.newBuilder()
                    .setMessageId(msg.getMessageId())
                    .build();
        }
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 7001;
        new CorfuProxyServer(port).start();
    }
}

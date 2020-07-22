package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.runtime.clients.TestChannelContext;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.view.Layout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by mwei on 12/13/15.
 */
@Slf4j
public class TestServerRouter implements IServerRouter, AutoCloseable {

    @Getter
    public List<CorfuMsg> responseMessages = new ArrayList<>();

    @Getter
    public Map<CorfuMsgType, AbstractServer> handlerMap = new HashMap<>();

    @Getter
    public ArrayList<AbstractServer> servers = new ArrayList<>();

    public List<TestRule> rules = new ArrayList<>();

    AtomicLong requestCounter;

    @Getter
    long serverEpoch;

    @Setter
    @Getter
    ServerContext serverContext;

    @Getter
    int port = 0;

    public TestServerRouter() {
        reset();
    }

    public TestServerRouter(int port) {
        reset();
        this.port = port;
    }

    public void reset() {
        close();
        this.responseMessages = new ArrayList<>();
        this.requestCounter = new AtomicLong();
        this.servers = new ArrayList<>();
        this.handlerMap = new ConcurrentHashMap<>();
        this.rules = new ArrayList<>();
    }

    @Override
    public void sendResponse(ChannelHandlerContext ctx, CorfuMsg inMsg, CorfuMsg outMsg) {
        outMsg.copyBaseFields(inMsg);
        outMsg.setEpoch(getServerEpoch());
        if (rules.stream()
                .map(x -> x.evaluate(outMsg, this))
                .allMatch(x -> x)) {
            if (ctx != null && ctx instanceof TestChannelContext) {
                ctx.writeAndFlush(outMsg);
            } else {
                this.responseMessages.add(outMsg);
            }
        }
    }

    @Override
    public void addServer(AbstractServer server) {
        servers.add(server);
        server.getHandler()
                .getHandledTypes()
                .forEach(msgType -> {
                    AbstractServer prevServer = handlerMap.put(msgType, server);
                    if (prevServer != null) {
                        prevServer.shutdown();
                    }
                    log.trace("Registered {} to handle messages of type {}", server, msgType);
                });
    }

    @Override
    public List<AbstractServer> getServers() {
        return servers;
    }

    public void sendServerMessage(CorfuMsg msg) {
        sendServerMessage(msg, null);
    }

    public void sendServerMessage(CorfuMsg msg, ChannelHandlerContext ctx) {
        AbstractServer as = handlerMap.get(msg.getMsgType());
        if (messageIsValid(msg, ctx)) {
            if (as != null) {
                // refactor and move threading to handler
                as.handleMessage(msg, ctx, this);
            }
            else {
                log.trace("Unregistered message of type {} sent to router", msg.getMsgType());
            }
        } else {
            log.trace("Message with wrong epoch {}, expected {}", msg.getEpoch(), serverEpoch);
        }
    }

    public void setServerEpoch(long serverEpoch) {
        this.serverEpoch = serverEpoch;
        getServers().forEach(s -> s.sealServerWithEpoch(serverEpoch));
    }

    @Override
    public Optional<Layout> getCurrentLayout() {
        if(getServerContext() == null) {
            throw new IllegalStateException("ServerContext should be set.");
        }
        return Optional.ofNullable(getServerContext().getCurrentLayout());
    }

    @Override
    public void close() {
        List<AbstractServer> handlers = new ArrayList<>(handlerMap.values());
        handlers.forEach(this::shutdownServer);

        servers.forEach(server -> {
            if (!handlers.contains(server)) {
                shutdownServer(server);
            }
        });
    }

    private void shutdownServer(AbstractServer server) {
        String serverName = server.getClass().getSimpleName();
        try {
            server.shutdown();
        } catch (Exception ex) {
            log.error("close: Failed to shutdown: {}", serverName);
        }
    }
}

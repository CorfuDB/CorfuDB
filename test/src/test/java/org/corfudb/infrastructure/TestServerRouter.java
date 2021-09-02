package org.corfudb.infrastructure;

import com.google.protobuf.TextFormat;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.service.CorfuProtocolMessage;
import org.corfudb.runtime.clients.TestChannelContext;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.view.Layout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by mwei on 12/13/15.
 */
@Slf4j
public class TestServerRouter implements IServerRouter {

    @Getter
    public List<ResponseMsg> protoResponseMessages;

    @Getter
    public Map<RequestPayloadMsg.PayloadCase, AbstractServer> requestTypeHandlerMap;

    @Getter
    public ArrayList<AbstractServer> servers;

    public List<TestRule> rules;

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
        this.protoResponseMessages = new ArrayList<>();
        this.requestCounter = new AtomicLong();
        this.servers = new ArrayList<>();
        // EnumMap is not thread-safe - https://docs.oracle.com/javase/7/docs/api/java/util/EnumMap.html
        this.requestTypeHandlerMap = Collections.synchronizedMap(new EnumMap<>(RequestPayloadMsg.PayloadCase.class));
        this.rules = new ArrayList<>();
    }

    /**
     * Send a response message through this router.
     *
     * @param response The response message to send.
     * @param ctx      The context of the channel handler.
     */
    public void sendResponse(ResponseMsg response, ChannelHandlerContext ctx) {
        // Set the server epoch; protobuf objects are immutable, hence create a new object
        ResponseMsg newResponse = CorfuProtocolMessage.getResponseMsg(
                HeaderMsg.newBuilder(response.getHeader()).setEpoch(getServerEpoch()).build(),
                response.getPayload());

        if (rules.stream()
                .allMatch(x -> x.evaluate(newResponse, this))) {
            if (ctx instanceof TestChannelContext) {
                ctx.writeAndFlush(newResponse);
                log.debug("sendResponse: Sent {} - {}", response.getPayload().getPayloadCase(),
                        TextFormat.shortDebugString(response.getHeader()));
            } else {
                this.protoResponseMessages.add(newResponse);
                log.debug("sendResponse: Added response - {} to protoResponseMessages List.",
                        TextFormat.shortDebugString(response.getHeader()));
            }
        }
    }

    @Override
    public void addServer(AbstractServer server) {
        servers.add(server);

        server.getHandlerMethods().getHandledTypes().forEach(x -> {
            requestTypeHandlerMap.put(x, server);
            log.trace("Registered {} to handle messages of type {}", server, x);
        });
    }

    @Override
    public List<AbstractServer> getServers() {
        return servers;
    }

    public void sendServerMessage(RequestMsg request, ChannelHandlerContext ctx) {
        AbstractServer as = requestTypeHandlerMap.get(request.getPayload().getPayloadCase());
        if (validateRequest(request, ctx)) {
            if (as != null) {
                // refactor and move threading to handler
                as.handleMessage(request, ctx, this);
            }
            else {
                log.trace("Unregistered message of type {} sent to router", request.getPayload().getPayloadCase());
            }
        } else {
            log.trace("Message with wrong epoch {}, expected {}", request.getHeader().getEpoch(), serverEpoch);
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
}

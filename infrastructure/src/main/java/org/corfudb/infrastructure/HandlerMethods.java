package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

/**
 * This class implements message handlers for {@link AbstractServer} implementations.
 * Servers define methods with signatures which follow the signature of the {@link HandlerMethod}
 * functional interface, and generate a handler using the
 * {@link this#generateHandler(MethodHandles.Lookup, AbstractServer)} method.
 *
 * <p>For maximum performance, make the handlers static whenever possible.
 * Handlers should be as short as possible (not block), since handler threads come from a
 * shared pool used by all servers. Blocking operations should be offloaded to I/O threads.
 *
 * <p>Created by mwei on 7/26/16.
 */
@Slf4j
public class HandlerMethods {

    /**
     * A functional interface for server message handlers. Server message handlers should
     * be fast and not block. If a handler blocks for an extended period of time, it will
     * exhaust the server's thread pool. I/O and other long operations should be handled
     * on another thread.
     */
    @FunctionalInterface
    public interface HandlerMethod<T extends CorfuMsg> {
        void handle(@Nonnull T corfuMsg,
                    @Nonnull ChannelHandlerContext ctx,
                    @Nonnull IServerRouter r);
    }

    /** The handler map. */
    private final Map<CorfuMsgType, HandlerMethod<CorfuMsg>> handlerMap;

    public void execute(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        if (!handlerMap.containsKey(msg.getMsgType())) {
            throw new IllegalStateException("Unknown type!");
        }

        handlerMap.get(msg.getMsgType()).handle(msg, ctx, r);
    }

    /** Get the types this handler will process.
     *
     * @return  A set containing the types this handler will process.
     */
    public Set<CorfuMsgType> getHandledTypes() {
        return handlerMap.keySet();
    }

    public void addType(CorfuMsgType type, HandlerMethod<CorfuMsg> handler) {
        handlerMap.put(type, handler);
    }

    /** Construct a new instance of HandlerMethods. */
    public HandlerMethods() {
        handlerMap = new EnumMap<>(CorfuMsgType.class);
    }
}

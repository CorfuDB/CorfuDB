package org.corfudb.util;

import io.netty.channel.ChannelHandlerContext;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.protocols.wireprotocol.CorfuMsg;

import java.lang.reflect.ParameterizedType;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * This class simplifies writing switch(msg.getType()) statements.
 *
 * For maximum performance, make the handlers static whenever possible.
 *
 * Created by mwei on 7/26/16.
 */
public class CorfuMsgHandler {

    @FunctionalInterface
    public interface Handler<T extends CorfuMsg> {
        void handle(T CorfuMsg, ChannelHandlerContext ctx, IServerRouter r);
    }

    /** The handler map. */
    private Map<CorfuMsg.CorfuMsgType, Handler> handlerMap;

    /** Construct a new instance of CorfuMsgHandler. */
    public CorfuMsgHandler() {
        handlerMap = new ConcurrentHashMap<>();
    }

    /** Add a handler to this message handler.
     *
     * @param messageType       The type of CorfuMsg this handler will handle.
     * @param handler           The handler itself.
     * @param <T>               A CorfuMsg type.
     * @return                  This handler, to support chaining.
     */
    @SuppressWarnings("unchecked")
    public <T extends CorfuMsg> CorfuMsgHandler
    addHandler(CorfuMsg.CorfuMsgType messageType, Handler<T> handler) {
        handlerMap.put(messageType, handler);
        return this;
    }

    /** Handle an incoming CorfuMsg.
     *
     * @param message   The message to handle.
     * @param ctx       The channel handler context.
     * @param r         The server router.
     * @return          True, if the message was handled.
     *                  False otherwise.
     */
    @SuppressWarnings("unchecked")
    public boolean handle(CorfuMsg message, ChannelHandlerContext ctx, IServerRouter r) {
        if (handlerMap.containsKey(message.getMsgType())) {
            handlerMap.get(message.getMsgType()).handle(message, ctx, r);
            return true;
        }
        return false;
    }

}

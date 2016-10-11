package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;

import java.lang.invoke.*;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class simplifies writing switch(msg.getType()) statements.
 *
 * For maximum performance, make the handlers static whenever possible.
 *
 * Created by mwei on 7/26/16.
 */
public class CorfuMsgHandler {

    @FunctionalInterface
    interface Handler<T extends CorfuMsg> {
        void handle(T CorfuMsg, ChannelHandlerContext ctx, IServerRouter r);
    }

    /** The handler map. */
    private Map<CorfuMsgType, Handler> handlerMap;

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
    addHandler(CorfuMsgType messageType, Handler<T> handler) {
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

    /** Generate handlers for a particular server.
     *
     * @param caller    The context that is being used. Call MethodHandles.lookup() to obtain.
     * @param o         The object that implements the server.
     * @return
     */
    public CorfuMsgHandler generateHandlers(final MethodHandles.Lookup caller, final Object o) {
        Arrays.stream(o.getClass().getDeclaredMethods())
                .filter(x -> x.isAnnotationPresent(ServerHandler.class))
                .forEach(x -> {
                    ServerHandler a = x.getAnnotation(ServerHandler.class);
                    if (!x.getParameterTypes()[0].isAssignableFrom(a.type().messageType.getRawType()))
                    {
                        throw new RuntimeException("Incorrect message type, expected " +
                                a.type().messageType.getRawType() + " but provided " + x.getParameterTypes()[0]);
                    }
                    if (handlerMap.containsKey(a.type())) {
                        throw new RuntimeException("Handler for " + a.type() + " already registered!");
                    }
                    // convert the method into a Java8 Lambda for maximum execution speed...
                    try {
                        if (Modifier.isStatic(x.getModifiers())) {
                            MethodHandle mh = caller.unreflect(x);
                            MethodType mt = mh.type().changeParameterType(0, CorfuMsg.class);
                            handlerMap.put(a.type(), (Handler) LambdaMetafactory.metafactory(caller,
                                    "handle", MethodType.methodType(Handler.class),
                                    mt, mh, mh.type())
                                    .getTarget().invokeExact());
                        } else {
                            // instance method, so we need to capture the type.
                            MethodType mt = MethodType.methodType(x.getReturnType(), x.getParameterTypes());
                            MethodHandle mh = caller.findVirtual(o.getClass(), x.getName(), mt);
                            MethodType mtGeneric = mh.type().changeParameterType(1, CorfuMsg.class);
                            handlerMap.put(a.type(), (Handler) LambdaMetafactory.metafactory(caller,
                                    "handle", MethodType.methodType(Handler.class, o.getClass()),
                                    mtGeneric.dropParameterTypes(0,1), mh, mh.type().dropParameterTypes(0,1))
                                    .getTarget().bindTo(o).invoke());
                        }
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                });
        return this;
    }

}

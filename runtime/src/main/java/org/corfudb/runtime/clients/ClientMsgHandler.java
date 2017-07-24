package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;

/**
 * Invokes the message handler to handle incoming messages.
 *
 * <p>Created by mwei on 7/27/16.
 */
@Slf4j
public class ClientMsgHandler {

    @FunctionalInterface
    public interface Handler<T extends CorfuMsg> {
        Object handle(T corfuMsg, ChannelHandlerContext ctx, IClientRouter r) throws Exception;
    }

    /**
     * The handler map.
     */
    private Map<CorfuMsgType, ClientMsgHandler.Handler> handlerMap;

    /**
     * The client.
     */
    private IClient client;

    /**
     * Construct a new instance of ClientMsgHandler.
     */
    public ClientMsgHandler(IClient client) {
        this.client = client;
        handlerMap = new ConcurrentHashMap<>();
    }

    /**
     * Add a handler to this message handler.
     *
     * @param messageType The type of CorfuMsg this handler will handle.
     * @param handler     The handler itself.
     * @param <T>         A CorfuMsg type.
     * @return This handler, to support chaining.
     */
    public <T extends CorfuMsg> ClientMsgHandler addHandler(CorfuMsgType messageType,
                                                            ClientMsgHandler.Handler<T> handler) {
        handlerMap.put(messageType, handler);
        return this;
    }

    /**
     * Handle an incoming CorfuMsg.
     *
     * @param message The message to handle.
     * @param ctx     The channel handler context.
     * @return True, if the message was handled. False otherwise.
     */
    @SuppressWarnings("unchecked")
    public boolean handle(CorfuMsg message, ChannelHandlerContext ctx) {
        if (handlerMap.containsKey(message.getMsgType())) {
            try {
                Object ret = handlerMap.get(message.getMsgType())
                        .handle(message, ctx, client.getRouter());
                if (ret != null) {
                    client.getRouter().completeRequest(message.getRequestID(), ret);
                }
            } catch (Exception ex) {
                client.getRouter().completeExceptionally(message.getRequestID(), ex);
            }
            return true;
        }
        return false;
    }


    /**
     * Generate handlers for a particular client.
     *
     * @param caller The context that is being used. Call MethodHandles.lookup() to obtain.
     * @param o      The object that implements the client.
     * @return Returns a handler to handle incoming messages to clients.
     */
    public ClientMsgHandler generateHandlers(final MethodHandles.Lookup caller, final Object o) {
        Arrays.stream(o.getClass().getDeclaredMethods())
                .filter(x -> x.isAnnotationPresent(ClientHandler.class))
                .forEach(x -> {
                    ClientHandler a = x.getAnnotation(ClientHandler.class);
                    if (!x.getParameterTypes()[0]
                            .isAssignableFrom(a.type().messageType.getRawType())) {
                        throw new RuntimeException("Incorrect message type, expected "
                                + a.type().messageType.getRawType() + " but provided "
                                + x.getParameterTypes()[0]);
                    }
                    if (handlerMap.containsKey(a.type())) {
                        throw new RuntimeException("Handler for " + a.type()
                                + " already registered!");
                    }
                    // convert the method into a Java8 Lambda for maximum execution speed...
                    try {
                        if (Modifier.isStatic(x.getModifiers())) {
                            MethodHandle mh = caller.unreflect(x);
                            MethodType mt = mh.type().changeParameterType(0, CorfuMsg.class);
                            handlerMap.put(a.type(), (ClientMsgHandler.Handler) LambdaMetafactory
                                    .metafactory(caller, "handle",
                                            MethodType.methodType(ClientMsgHandler.Handler.class),
                                            mt, mh, mh.type())
                                    .getTarget().invokeExact());
                        } else {
                            // instance method, so we need to capture the type.
                            MethodType mt = MethodType
                                    .methodType(x.getReturnType(), x.getParameterTypes());
                            MethodHandle mh = caller.findVirtual(o.getClass(), x.getName(), mt);
                            MethodType mtGeneric = mh.type()
                                    .changeParameterType(1, CorfuMsg.class)
                                    .changeReturnType(Object.class);
                            handlerMap.put(a.type(), (ClientMsgHandler.Handler) LambdaMetafactory
                                    .metafactory(caller, "handle",
                                            MethodType.methodType(ClientMsgHandler.Handler.class,
                                                    o.getClass()),
                                            mtGeneric.dropParameterTypes(0, 1), mh, mh.type()
                                                    .dropParameterTypes(0, 1))
                                    .getTarget().bindTo(o).invoke());
                        }
                    } catch (Throwable e) {
                        log.error("Exception during incoming message handling", e);
                        throw new RuntimeException(e);
                    }
                });
        return this;
    }

    /**
     * Get the types this handler will handle.
     *
     * @return The types this handler will handle.
     */
    public Set<CorfuMsgType> getHandledTypes() {
        return handlerMap.keySet();
    }
}

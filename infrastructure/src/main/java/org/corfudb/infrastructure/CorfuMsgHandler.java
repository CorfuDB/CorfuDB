package org.corfudb.infrastructure;

import com.codahale.metrics.Timer;

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
import org.corfudb.protocols.wireprotocol.ExceptionMsg;
import org.corfudb.util.MetricsUtils;

/**
 * This class simplifies writing switch(msg.getType()) statements.
 *
 * <p>For maximum performance, make the handlers static whenever possible.
 *
 * <p>Created by mwei on 7/26/16.
 */
@Slf4j
public class CorfuMsgHandler {

    @FunctionalInterface
    interface Handler<T extends CorfuMsg> {
        void handle(T corfuMsg, ChannelHandlerContext ctx, IServerRouter r,
                    boolean isMetricsEnabled);
    }

    /** The handler map. */
    private Map<CorfuMsgType, Handler> handlerMap;

    /** Get the types this handler will handle.
     *
     * @return  A set containing the types this handler will handle.
     */
    public Set<CorfuMsgType> getHandledTypes() {
        return handlerMap.keySet();
    }

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
    public <T extends CorfuMsg> CorfuMsgHandler addHandler(CorfuMsgType messageType,
                                                           Handler<T> handler) {
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
    public boolean handle(CorfuMsg message, ChannelHandlerContext ctx, IServerRouter r,
                          boolean isMetricsEnabled) {
        if (handlerMap.containsKey(message.getMsgType())) {
            try {
                handlerMap.get(message.getMsgType()).handle(message, ctx, r, isMetricsEnabled);
            } catch (Exception ex) {
                log.error("handle: Unhandled exception processing {} message",
                        message.getMsgType(), ex);
                r.sendResponse(ctx, message,
                        CorfuMsgType.ERROR_SERVER_EXCEPTION.payloadMsg(new ExceptionMsg(ex)));
            }
            return true;
        }
        return false;
    }

    /** Generate handlers for a particular server.
     *
     * @param caller    The context that is being used. Call MethodHandles.lookup() to obtain.
     * @param o         The object that implements the server.
     * @return          new message handler for caller class
     */
    public CorfuMsgHandler generateHandlers(final MethodHandles.Lookup caller, final Object o) {
        Arrays.stream(o.getClass().getDeclaredMethods())
                .filter(x -> x.isAnnotationPresent(ServerHandler.class))
                .forEach(x -> {
                    ServerHandler a = x.getAnnotation(ServerHandler.class);

                    if (!x.getParameterTypes()[0].isAssignableFrom(a.type().messageType
                            .getRawType())) {
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
                        Handler h;
                        if (Modifier.isStatic(x.getModifiers())) {
                            MethodHandle mh = caller.unreflect(x);
                            MethodType mt = mh.type().changeParameterType(0, CorfuMsg.class);
                            h = (Handler) LambdaMetafactory.metafactory(caller,
                                    "handle", MethodType.methodType(Handler.class),
                                    mt, mh, mh.type())
                                    .getTarget().invokeExact();

                        } else {
                            // instance method, so we need to capture the type.
                            MethodType mt = MethodType.methodType(x.getReturnType(),
                                    x.getParameterTypes());
                            MethodHandle mh = caller.findVirtual(o.getClass(), x.getName(), mt);
                            MethodType mtGeneric = mh.type().changeParameterType(1, CorfuMsg.class);
                            h = (Handler) LambdaMetafactory.metafactory(caller,
                                    "handle", MethodType.methodType(Handler.class, o.getClass()),
                                    mtGeneric.dropParameterTypes(0,1), mh,
                                    mh.type().dropParameterTypes(0,1)).getTarget()
                                    .bindTo(o).invoke();
                        }

                        // If there is an annotation element for opTimer that is not "", then
                        // find/create its timer *outside* of the lambda that we create.
                        // The lambda may be called very frequently, so avoid touching the metrics
                        // registry on a hot code path.
                        final Timer t;
                        if (!a.opTimer().equals("")) {
                            t = ServerContext.getMetrics().timer(a.opTimer());
                        } else {
                            t = null;
                        }
                        // Now create the lambda that wraps the lambda-like-thing that's
                        // stored in 'h' and insert it into the handlerMap.
                        handlerMap.put(a.type(),
                                (CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r,
                                 boolean isMetricsEnabled) -> {
                                    try (Timer.Context timerCxt
                                                 = MetricsUtils.getConditionalContext(
                                            t != null && isMetricsEnabled, t)) {
                                        h.handle(msg, ctx, r, isMetricsEnabled);
                                    }
                            });
                    } catch (Throwable e) {
                        log.error("Exception during incoming message handling", e);
                        throw new RuntimeException(e);
                    }
                });
        return this;
    }

}

package org.corfudb.infrastructure;

import com.codahale.metrics.Timer;
import io.netty.channel.ChannelHandlerContext;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.ExceptionMsg;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.util.MetricsUtils;

/**
 * This class implements message handlers for {@link AbstractServer} implementations.
 * Servers define methods with signatures which follow the signature of the {@link Handler}
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
public class CorfuMsgHandler {

    /**
     * A functional interface for server message handlers. Server message handlers should
     * be fast and not block. If a handler blocks for an extended period of time, it will
     * exhaust the server's thread pool. I/O and other long operations should be handled
     * on another thread.
     */
    @FunctionalInterface
    interface Handler<T extends CorfuMsg> {
        void handle(@Nonnull T corfuMsg,
                    @Nonnull ChannelHandlerContext ctx,
                    @Nonnull IServerRouter r);
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

    /** Get a handler for a specific message type.
     *
     * @param type  The type to retrieve a handler for.
     * @return      A handler for the requested message type.
     */
    @SuppressWarnings("unchecked")
    public Handler<CorfuMsg> getHandler(CorfuMsgType type) {
        return handlerMap.get(type);
    }

    /** Construct a new instance of CorfuMsgHandler. */
    public CorfuMsgHandler() {
        handlerMap = new EnumMap<>(CorfuMsgType.class);
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
    public boolean handle(CorfuMsg message, ChannelHandlerContext ctx, IServerRouter r) {
        // Get the handler for the message type
        final Handler<CorfuMsg> handler = handlerMap.get(message.getMsgType());
        // If present...
        if (handler != null) {
            try {
                handler.handle(message, ctx, r);
            } catch (Exception ex) {
                // Log the exception during handling
                log.error("handle: Unhandled exception processing {} message",
                        message.getMsgType(), ex);
                r.sendResponse(ctx, message,
                        CorfuMsgType.ERROR_SERVER_EXCEPTION.payloadMsg(new ExceptionMsg(ex)));
            }
            // Otherwise log the unexpected message.
        } else {
            log.error("handle: Unregistered message type {}", message.getMsgType());
        }

        return handler != null;
    }

    /** Generate handlers for a particular server.
     *
     * @param caller    The context that is being used. Call MethodHandles.lookup() to obtain.
     * @param server    The object that implements the server.
     * @return          New message handler for caller class
     */
    public static CorfuMsgHandler generateHandler(@Nonnull final MethodHandles.Lookup caller,
            @Nonnull final AbstractServer server) {
        CorfuMsgHandler handler = new CorfuMsgHandler();
        Arrays.stream(server.getClass().getDeclaredMethods())
            .filter(method -> method.isAnnotationPresent(ServerHandler.class))
            .forEach(method -> handler.registerMethod(caller, server, method));
        return handler;
    }



    /** Takes a method annotated with the {@link org.corfudb.infrastructure.ServerHandler}
     *  annotation, converts it into a lambda, and registers it in the {@code handlerMap}.
     * @param caller   The context that is being used. Call MethodHandles.lookup() to obtain.
     * @param server   The object that implements the server.
     * @param method   The method to be registered. Must be annotated with the
     *                 {@link ServerHandler} annotation.
     */
    @SuppressWarnings("unchecked")
    private void registerMethod(@Nonnull final MethodHandles.Lookup caller,
            @Nonnull final AbstractServer server,
            @Nonnull final Method method) {
        final ServerHandler annotation = method.getAnnotation(ServerHandler.class);

        if (!method.getParameterTypes()[0].isAssignableFrom(annotation.type().messageType
                .getRawType())) {
            throw new UnrecoverableCorfuError("Incorrect message type, expected "
                + annotation.type().messageType.getRawType() + " but provided "
                + method.getParameterTypes()[0]);
        }
        if (handlerMap.containsKey(annotation.type())) {
            throw new UnrecoverableCorfuError("Handler for " + annotation.type()
                + " already registered!");
        }
        // convert the method into a Java8 Lambda for maximum execution speed...
        try {
            Handler<CorfuMsg> h;
            if (Modifier.isStatic(method.getModifiers())) {
                MethodHandle mh = caller.unreflect(method);
                MethodType mt = mh.type().changeParameterType(0, CorfuMsg.class);
                h = (Handler<CorfuMsg>) LambdaMetafactory.metafactory(caller,
                    "handle", MethodType.methodType(Handler.class),
                    mt, mh, mh.type())
                    .getTarget().invokeExact();

            } else {
                // instance method, so we need to capture the type.
                MethodType mt = MethodType.methodType(method.getReturnType(),
                        method.getParameterTypes());
                MethodHandle mh = caller.findVirtual(server.getClass(), method.getName(), mt);
                MethodType mtGeneric = mh.type().changeParameterType(1, CorfuMsg.class);
                h = (Handler<CorfuMsg>) LambdaMetafactory.metafactory(caller,
                    "handle",
                    MethodType.methodType(Handler.class, server.getClass()),
                    mtGeneric.dropParameterTypes(0, 1), mh,
                    mh.type().dropParameterTypes(0, 1)).getTarget()
                    .bindTo(server).invoke();
            }
            // Install pre-conditions on handler
            final Handler<CorfuMsg> handler =
                    generateConditionalHandler(server, annotation.type(), h);
            // Install the handler in the map
            handlerMap.put(annotation.type(), handler);
        } catch (Throwable e) {
            log.error("Exception during message handler registration", e);
            throw new UnrecoverableCorfuError(e);

        }
    }

    /** Generate a conditional handler, which instruments the handler with metrics if enabled
     *  and checks whether the server is shutdown and ready.
     * @param server        The {@link AbstractServer} the message is being handled for.
     * @param type          The {@link CorfuMsgType} the message is being handled for.
     * @param handler       The {@link Handler} which handles the message.
     * @return              A new {@link Handler} which conditionally executes the handler
     *                      based on preconditions (whether the server is shutdown/ready).
     */
    private Handler<CorfuMsg> generateConditionalHandler(@Nonnull final AbstractServer server,
            @Nonnull final CorfuMsgType type,
            @Nonnull final Handler<CorfuMsg> handler) {
        if (!MetricsUtils.isMetricsCollectionEnabled()) {
            // If metrics are disabled, register the handler without instrumentation.
            return (msg, ctx, r) -> {
                if (server.isShutdown()) {
                    log.warn("Server received {} but is shutdown.", msg.getMsgType().toString());
                    r.sendResponse(ctx, msg, CorfuMsgType.ERROR_SHUTDOWN_EXCEPTION.msg());
                    return;
                }

                if (!server.isServerReadyToHandleMsg(msg)) {
                    r.sendResponse(ctx, msg, CorfuMsgType.NOT_READY.msg());
                    return;
                }

                handler.handle(msg, ctx, r);
            };
        } else {
            // Otherwise, generate a timer based on the operation name
            final Timer timer = ServerContext.getMetrics().timer(type.toString());
            // And wrap the handler around a new lambda which measures the execution time.
            return (msg, ctx, r) -> {
                if (server.isShutdown()) {
                    log.warn("Server received {} but is shutdown.", msg.getMsgType().toString());
                    r.sendResponse(ctx, msg, CorfuMsgType.ERROR_SHUTDOWN_EXCEPTION.msg());
                    return;
                }

                if (!server.isServerReadyToHandleMsg(msg)) {
                    r.sendResponse(ctx, msg, CorfuMsgType.NOT_READY.msg());
                    return;
                }

                try (Timer.Context context = MetricsUtils.getConditionalContext(timer)) {
                    handler.handle(msg, ctx, r);
                }
            };
        }
    }
}

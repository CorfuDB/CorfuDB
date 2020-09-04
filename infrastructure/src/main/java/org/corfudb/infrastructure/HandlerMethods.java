package org.corfudb.infrastructure;

import com.codahale.metrics.Timer;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.ExceptionMsg;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.MetricsUtils;

import javax.annotation.Nonnull;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
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

    private final Map<CorfuMsgType, String> timerNameCache = new HashMap<>();

    /** The handler map. */
    private final Map<CorfuMsgType, HandlerMethod> handlerMap;

    /**
     * A functional interface for server message handlers. Server message handlers should
     * be fast and not block. If a handler blocks for an extended period of time, it will
     * exhaust the server's thread pool. I/O and other long operations should be handled
     * on another thread.
     */
    @FunctionalInterface
    interface HandlerMethod<T extends CorfuMsg> {
        void handle(@Nonnull T corfuMsg,
                    @Nonnull ChannelHandlerContext ctx,
                    @Nonnull IServerRouter r);
    }

    /** Get the types this handler will handle.
     *
     * @return  A set containing the types this handler will handle.
     */
    public Set<CorfuMsgType> getHandledTypes() {
        return handlerMap.keySet();
    }

    /** Construct a new instance of HandlerMethods. */
    public HandlerMethods() {
        handlerMap = new EnumMap<>(CorfuMsgType.class);
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
    public void handle(CorfuMsg message, ChannelHandlerContext ctx, IServerRouter r) {
        final HandlerMethod<CorfuMsg> handler = handlerMap.get(message.getMsgType());
        try {
            handler.handle(message, ctx, r);
        } catch (Exception ex) {
            // Log the exception during handling
            log.error("handle: Unhandled exception processing {} message",
                    message.getMsgType(), ex);
            r.sendResponse(ctx, message, CorfuMsgType.ERROR_SERVER_EXCEPTION.payloadMsg(new ExceptionMsg(ex)));
        }
    }

    /** Handle an incoming CorfuMsg for any generic transport layer (not only Netty).
     *
     * @param message   The message to handle.
     * @param r         The server router.
     * @return          True, if the message was handled.
     *                  False otherwise.
     */
    @SuppressWarnings("unchecked")
    public void handle(CorfuMsg message, IServerRouter r) {
       handle(message, null, r);
    }

    /** Generate handlers for a particular server.
     *
     * @param caller    The context that is being used. Call MethodHandles.lookup() to obtain.
     * @param server    The object that implements the server.
     * @return          New message handler for caller class
     */
    public static HandlerMethods generateHandler(@Nonnull final MethodHandles.Lookup caller,
                                                 @Nonnull final AbstractServer server) {
        HandlerMethods handler = new HandlerMethods();
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
            throw new UnrecoverableCorfuError("HandlerMethod for " + annotation.type()
                + " already registered!");
        }
        // convert the method into a Java8 Lambda for maximum execution speed...
        try {
            HandlerMethod<CorfuMsg> h;
            if (Modifier.isStatic(method.getModifiers())) {
                MethodHandle mh = caller.unreflect(method);
                MethodType mt = mh.type().changeParameterType(0, CorfuMsg.class);
                h = (HandlerMethod<CorfuMsg>) LambdaMetafactory.metafactory(caller,
                    "handle", MethodType.methodType(HandlerMethod.class),
                    mt, mh, mh.type())
                    .getTarget().invokeExact();

            } else {
                // instance method, so we need to capture the type.
                MethodType mt = MethodType.methodType(method.getReturnType(),
                        method.getParameterTypes());
                MethodHandle mh = caller.findVirtual(server.getClass(), method.getName(), mt);
                MethodType mtGeneric = mh.type().changeParameterType(1, CorfuMsg.class);
                h = (HandlerMethod<CorfuMsg>) LambdaMetafactory.metafactory(caller,
                    "handle",
                    MethodType.methodType(HandlerMethod.class, server.getClass()),
                    mtGeneric.dropParameterTypes(0, 1), mh,
                    mh.type().dropParameterTypes(0, 1)).getTarget()
                    .bindTo(server).invoke();
            }
            // Install pre-conditions on handler
            final HandlerMethod<CorfuMsg> handler = generateConditionalHandler(annotation.type(), h);
            // Install the handler in the map
            handlerMap.put(annotation.type(), handler);
        } catch (Throwable e) {
            log.error("Exception during message handler registration", e);
            throw new UnrecoverableCorfuError(e);

        }
    }

    /** Generate a conditional handler, which instruments the handler with metrics if configured
     * as enabled and checks whether the server is shutdown and ready.
     * @param type          The {@link CorfuMsgType} the message is being handled for.
     * @param handler       The {@link HandlerMethod} which handles the message.
     * @return              A new {@link HandlerMethod} which conditionally executes the handler
     *                      based on preconditions (whether the server is shutdown/ready).
     */
    private HandlerMethod<CorfuMsg> generateConditionalHandler(@Nonnull final CorfuMsgType type,
                                                               @Nonnull final HandlerMethod<CorfuMsg> handler) {
        // Generate a timer based on the Corfu message type
        final Timer timer = getTimer(type);

        // Register the handler. Depending on metrics collection configuration by MetricsUtil,
        // handler will be instrumented by the metrics context.
        return (msg, ctx, r) -> {
            try (Timer.Context context = MetricsUtils.getConditionalContext(timer)) {
                handler.handle(msg, ctx, r);
            }
        };
    }

    // Create a timer using cached timer name for the corresponding type
    private Timer getTimer(@Nonnull CorfuMsgType type) {
        timerNameCache.computeIfAbsent(type,
                                       aType -> (CorfuComponent.INFRA_MSG_HANDLER +
                                                 aType.name().toLowerCase()));

        return ServerContext.getMetrics().timer(timerNameCache.get(type));
    }
}

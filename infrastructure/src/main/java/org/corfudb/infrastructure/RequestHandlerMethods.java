package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg.PayloadCase;

import javax.annotation.Nonnull;
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

import static org.corfudb.protocols.CorfuProtocolServerErrors.getUnknownErrorMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;

@Slf4j
public class RequestHandlerMethods {

    private final Map<PayloadCase, String> timerNameCache = new EnumMap<>(PayloadCase.class);

    /**
     * The handler map.
     */
    private final Map<PayloadCase, HandlerMethod> handlerMap;

    /**
     * A functional interface for server request handlers. Server request handlers should
     * be fast and not block. If a handler blocks for an extended period of time, it will
     * exhaust the server's thread pool. I/O and other long operations should be handled
     * on another thread.
     */
    @FunctionalInterface
    public interface HandlerMethod {
        void handle(@Nonnull RequestMsg req,
                    @Nonnull ChannelHandlerContext ctx,
                    @Nonnull IServerRouter r);
    }

    /**
     * Get the types of requests this handler will handle.
     *
     * @return  A set containing the types of requests this handler will handle.
     */
    public Set<PayloadCase> getHandledTypes() {
        return handlerMap.keySet();
    }

    /**
     * Construct a new instance of RequestHandlerMethods.
     */
    public RequestHandlerMethods() {
        handlerMap = new EnumMap<>(PayloadCase.class);
    }

    /**
     * Given a {@link RequestMsg} return the corresponding handler.
     *
     * @param request   The request message to handle.
     * @return          The appropriate {@link HandlerMethod}.
     */
    protected HandlerMethod getHandler(RequestMsg request) {
        return handlerMap.get(request.getPayload().getPayloadCase());
    }

    /**
     * Handle an incoming Corfu request message.
     *
     * @param req       The request message to handle.
     * @param ctx       The channel handler context.
     * @param r         The server router.
     */
    public void handle(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        final HandlerMethod handler = getHandler(req);
        try {
            handler.handle(req, ctx, r);
        } catch (Exception e) {
            log.error("handle[{}]: Unhandled exception processing {} request",
                    req.getHeader().getRequestId(), req.getPayload().getPayloadCase(), e);

            HeaderMsg responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
            r.sendResponse(getResponseMsg(responseHeader, getUnknownErrorMsg(e)), ctx);
        }
    }

    /**
     * Generate handlers for a particular server.
     *
     * @param caller    The context that is being used. Call MethodHandles.lookup() to obtain.
     * @param server    The object that implements the server.
     * @return          New request handlers for caller class.
     */
    public static RequestHandlerMethods generateHandler(@Nonnull final MethodHandles.Lookup caller,
                                                        @NonNull final AbstractServer server) {
        RequestHandlerMethods handler = new RequestHandlerMethods();
        Arrays.stream(server.getClass().getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(RequestHandler.class))
                .forEach(method -> handler.registerMethod(caller, server, method));
        return handler;
    }

    private void registerMethod(@Nonnull final MethodHandles.Lookup caller,
                                @Nonnull final AbstractServer server,
                                @Nonnull final Method method) {
        final RequestHandler annotation = method.getAnnotation(RequestHandler.class);

        if (handlerMap.containsKey(annotation.type())) {
            throw new IllegalStateException("HandlerMethod for " + annotation.type() + " already registered!");
        }

        try {
            HandlerMethod h;
            if (Modifier.isStatic(method.getModifiers())) {
                MethodHandle mh = caller.unreflect(method);
                h = (HandlerMethod) LambdaMetafactory.metafactory(caller,
                        "handle", MethodType.methodType(HandlerMethod.class),
                        mh.type(), mh, mh.type()).getTarget().invokeExact();
            } else {
                // Instance method, so we need to capture the type.
                MethodType mt = MethodType.methodType(method.getReturnType(), method.getParameterTypes());
                MethodHandle mh = caller.findVirtual(server.getClass(), method.getName(), mt);

                // Note: mtt captures the return type as well as the argument types of the annotated handler
                // method. These should be consistent with the handle method described by the HandlerMethod
                // functional interface.
                MethodType mtt = mh.type().dropParameterTypes(0, 1);
                h = (HandlerMethod) LambdaMetafactory.metafactory(caller, "handle",
                        MethodType.methodType(HandlerMethod.class, server.getClass()),
                        mtt, mh, mtt).getTarget().bindTo(server).invoke();
            }

            // Install pre-conditions on handler
            final HandlerMethod handler = generateConditionalHandler(annotation.type(), h);
            // Install the handler in the map
            handlerMap.put(annotation.type(), handler);
        } catch (Throwable e) {
            throw new UnrecoverableCorfuError("Exception during request handler registration", e);
        }
    }

    /**
     * Generate a conditional handler, which instruments the handler with metrics if
     * configured as enabled and checks whether the server is shutdown and ready.
     * @param type          The type the request message is being handled for.
     * @param handler       The {@link RequestHandlerMethods.HandlerMethod} which handles the message.
     * @return              A new {@link RequestHandlerMethods.HandlerMethod} which conditionally executes the
     *                      handler based on preconditions (whether the server is shutdown/ready).
     */
    private HandlerMethod generateConditionalHandler(@NonNull final PayloadCase type,
                                                     @NonNull final HandlerMethod handler) {
        // Generate a timer name based on the Corfu request type
        String timerName = getTimerName(type);
        // Register the handler. Depending on metrics collection configuration by MetricsUtil,
        // handler will be instrumented by the metrics context.
        return (req, ctx, r) -> MicroMeterUtils.time(() -> handler.handle(req, ctx, r), timerName);
    }

    // Create a timer using cached timer name for the corresponding type
    private String getTimerName(@Nonnull PayloadCase type) {
        timerNameCache.computeIfAbsent(type,
                aType -> ("corfu.infrastructure.message-handler." +
                        aType.name().toLowerCase()));
        return timerNameCache.get(type);
    }
}

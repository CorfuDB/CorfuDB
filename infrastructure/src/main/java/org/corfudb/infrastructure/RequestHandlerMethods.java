package org.corfudb.infrastructure;

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
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg.PayloadCase;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;

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
     * Handle an incoming Corfu request message.
     *
     * @param req       The request message to handle.
     * @param ctx       The channel handler context.
     * @param r         The server router.
     */
    @SuppressWarnings("unchecked")
    public void handle(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        final HandlerMethod handler = handlerMap.get(req.getPayload().getPayloadCase());
        try {
            handler.handle(req, ctx, r);
        } catch (Exception e) {
            log.error("handle[{}]: Unhandled exception processing {} request",
                    req.getHeader().getRequestId(), req.getPayload().getPayloadCase(), e);

            HeaderMsg responseHeader = getHeaderMsg(req.getHeader(), false, true);
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

            final HandlerMethod handler = h::handle;
            handlerMap.put(annotation.type(), handler);
        } catch (Throwable e) {
            throw new UnrecoverableCorfuError("Exception during request handler registration", e);
        }
    }
}

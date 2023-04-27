package org.corfudb.infrastructure.logreplication.infrastructure.msghandlers;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.infrastructure.logreplication.transport.IClientServerRouter;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;

import javax.annotation.Nonnull;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ReplicationHandlerMethods {

    private final Map<String, String> timerNameCache = new HashMap<>();

    /**
     * The request handler map.
     */
    private final Map<RequestPayloadMsg.PayloadCase, HandlerMethod> requestHandlerMap;

    /**
     * The response handler map.
     */
    private final Map<ResponsePayloadMsg.PayloadCase, HandlerMethod> responseHandlerMap;

    /**
     * A functional interface for clinet/server request/response handlers. The handlers should
     * be fast and not block. If a handler blocks for an extended period of time, it will
     * exhaust the server's thread pool. I/O and other long operations should be handled
     * on another thread.
     */
    @FunctionalInterface
    public interface HandlerMethod {
        void handle(CorfuMessage.RequestMsg req,
                    CorfuMessage.ResponseMsg res,
                    @Nonnull IClientServerRouter r);
    }

    /**
     * Construct a new instance of ReplicationHandlerMethods.
     */
    public ReplicationHandlerMethods() {
        requestHandlerMap = new ConcurrentHashMap<>();
        responseHandlerMap = new ConcurrentHashMap<>();
    }

    /**
     * Given a {@link CorfuMessage.RequestMsg} return the corresponding handler.
     *
     * @param request   The request message to handle.
     * @return          The appropriate {@link HandlerMethod}.
     */
    protected HandlerMethod getRequestHandler(CorfuMessage.RequestMsg request) {
        return requestHandlerMap.get(request.getPayload().getPayloadCase());
    }

    /**
     * Given a {@link CorfuMessage.ResponseMsg} return the corresponding handler.
     *
     * @param response   The respose message to handle.
     * @return          The appropriate {@link HandlerMethod}.
     */
    protected HandlerMethod getResponseHandler(CorfuMessage.ResponseMsg response) {
        return responseHandlerMap.get(response.getPayload().getPayloadCase());
    }

    /**
     * Handle an incoming Corfu request message.
     *
     * @param req       The request message to handle.
     * @param r         The server router.
     */
    @SuppressWarnings("unchecked")
    public void handle(CorfuMessage.RequestMsg req, CorfuMessage.ResponseMsg res, IClientServerRouter r) {
        final HandlerMethod handler = req != null ? getRequestHandler(req) : getResponseHandler(res);

        try {
            handler.handle(req, res, r);
        } catch (Exception e) {
            if (req != null) {
                log.error("handle[{}]: Unhandled exception processing {} request",
                        req.getHeader().getRequestId(), req.getPayload().getPayloadCase(), e);
            } else {
                log.error("handle[{}]: Unhandled exception processing {} response",
                        res.getHeader().getRequestId(), res.getPayload().getPayloadCase(), e);
            }
        }
    }

    /**
     * Generate handlers for a particular server.
     *
     * @param caller    The context that is being used. Call MethodHandles.lookup() to obtain.
     * @param server    The object that implements the server.
     * @return          New request handlers for caller class.
     */
    public static ReplicationHandlerMethods generateHandler(@Nonnull final MethodHandles.Lookup caller,
                                                            @NonNull final LogReplicationAbstractServer server) {
        ReplicationHandlerMethods handler = new ReplicationHandlerMethods();
        Arrays.stream(server.getClass().getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(LogReplicationRequestHandler.class) ||
                        method.isAnnotationPresent(LogReplicationResponseHandler.class))
                .forEach(method -> handler.registerMethod(caller, server, method));
        return handler;
    }

    private void registerMethod(@Nonnull final MethodHandles.Lookup caller,
                                @Nonnull final LogReplicationAbstractServer server,
                                @Nonnull final Method method) {
        RequestPayloadMsg.PayloadCase requestType = null;
        ResponsePayloadMsg.PayloadCase responseType = null;
        String payloadString;

        // check if the method is already registered
        if(method.isAnnotationPresent(LogReplicationRequestHandler.class)) {
            requestType = method.getAnnotation(LogReplicationRequestHandler.class).requestType();
            if(requestHandlerMap.containsKey(requestType)) {
                throw new IllegalStateException("Request handlerMethod for " + requestType + " already registered!");
            }
            payloadString = requestType.toString();
        } else {
            responseType = method.getAnnotation(LogReplicationResponseHandler.class).responseType();
            if(responseHandlerMap.containsKey(responseType)) {
                throw new IllegalStateException("Response handlerMethod for " + responseType + " already registered!");
            }
            payloadString = responseType.toString();
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
            final HandlerMethod handler = generateConditionalHandler(payloadString, h, requestType != null);
            // Install the handler in the map
            if (requestType != null) {
                requestHandlerMap.put(requestType, handler);
            } else {
                responseHandlerMap.put(responseType, handler);
            }
        } catch (Throwable e) {
            throw new UnrecoverableCorfuError("Exception during message handler registration", e);
        }
    }

    /**
     * Generate a conditional handler, which instruments the handler with metrics if
     * configured as enabled and checks whether the server is shutdown and ready.
     * @param payloadType          The type the message is being handled for.
     * @param handler       The {@link HandlerMethod} which handles the message.
     * @return              A new {@link HandlerMethod} which conditionally executes the
     *                      handler based on preconditions (whether the server is shutdown/ready).
     */
    private HandlerMethod generateConditionalHandler(@NonNull final String payloadType,
                                                     @NonNull final HandlerMethod handler, boolean isRequestType) {
        if (isRequestType) {
            // Generate a timer name based on the LR message type
            String timerName = getTimerName(payloadType);

            // Register the request handler. Depending on metrics collection configuration by MetricsUtil,
            // handler will be instrumented by the metrics context.
            return (req, res, r) -> MicroMeterUtils.time(() -> handler.handle(req, res, r), timerName);
        }

        return handler;
    }

    // Create a timer using cached timer name for the corresponding type
    private String getTimerName(@Nonnull String type) {
        timerNameCache.computeIfAbsent(type,
                aType -> ("corfu.infrastructure.log-replication-message-handler." +
                        aType.toLowerCase()));
        return timerNameCache.get(type);
    }
}

package org.corfudb.runtime.clients;

import com.google.protobuf.TextFormat;
import io.netty.channel.ChannelHandlerContext;
import lombok.NonNull;
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

import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.ServerErrors.ServerErrorMsg.ErrorCase;

/**
 * Invokes the message handler to handle Protobuf responses from server.
 */
@Slf4j
public class ClientResponseHandler {

    @FunctionalInterface
    public interface Handler {
        Object handle(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) throws Exception;
    }

    /**
     * The handler map for normal ResponseMsg.
     */
    private final Map<PayloadCase, Handler> handlerMap;

    /**
     * The handler map for ServerErrorMsg.
     */
    private final Map<ErrorCase, Handler> errorHandlerMap;

    /**
     * The client.
     */
    private final IClient client;

    /**
     * Construct a new instance of ClientResponseHandler.
     *
     * @param client The client that this ClientResponseHandler will register.
     */
    public ClientResponseHandler(IClient client) {
        this.client = client;
        this.handlerMap = new ConcurrentHashMap<>();
        this.errorHandlerMap = new ConcurrentHashMap<>();
    }

    /**
     * Construct a new instance of ClientResponseHandler.
     *
     * @param client The client that this ClientResponseHandler will register.
     */
    public ClientResponseHandler(IClient client, Map<PayloadCase, Handler> handlerMap) {
        this.client = client;
        this.handlerMap = handlerMap;
        this.errorHandlerMap = new ConcurrentHashMap<>();
    }

    /**
     * Handle an incoming Response from server.
     *
     * @param response The Response to handle.
     * @param ctx The channel handler context.
     * @return True if the message was handled successfully.
     */
    public boolean handle(ResponseMsg response, ChannelHandlerContext ctx) {
        IClientRouter router = client.getRouter();
        final long requestId = response.getHeader().getRequestId();
        PayloadCase payloadCase = response.getPayload().getPayloadCase();

        if (log.isTraceEnabled()) {
            log.trace("Received response from the server - {}",
                    TextFormat.shortDebugString(response));
        }

        if (payloadCase == PayloadCase.SERVER_ERROR) {
            ErrorCase errorCase = response.getPayload().getServerError().getErrorCase();
            if (errorHandlerMap.containsKey(errorCase)) {
                try{
                    // For errors, we always want to completeExceptionally
                    errorHandlerMap.get(errorCase).handle(response, ctx, router);
                } catch (Throwable e) {
                    log.warn("Server threw exception for {} with request_id: {}",
                            response.getPayload().getPayloadCase(),
                            response.getHeader().getRequestId());
                    router.completeExceptionally(requestId, e);
                }
            }
        } else if (handlerMap.containsKey(payloadCase)) {
            try {
                Object ret = handlerMap.get(payloadCase).handle(response, ctx, router);
                if (ret != null) {
                    router.completeRequest(requestId, ret);
                }
            } catch (Throwable e) {
                router.completeExceptionally(requestId, e);
            }
            return true;
        }
        return false;
    }

    public ClientResponseHandler generateHandlers(@NonNull final MethodHandles.Lookup caller,
                                                  @NonNull final Object o) {
        Arrays.stream(o.getClass().getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(ResponseHandler.class))
                .forEach(method -> {
                    ResponseHandler handler = method.getAnnotation(ResponseHandler.class);
                    if (!method.getParameterTypes()[0]
                            .isAssignableFrom(ResponseMsg.class)) {
                        throw new RuntimeException("Incorrect message type, expected "
                                + ResponseMsg.class + " but provided "
                                + method.getParameterTypes()[0]);
                    }

                    if (handlerMap.containsKey(handler.type())) {
                        throw new RuntimeException("Handler for " + handler.type()
                                + " already registered!");
                    }

                    // Convert the method into a Java8 Lambda for maximum execution speed...
                    try {
                        if (Modifier.isStatic(method.getModifiers())) {
                            MethodHandle mh = caller.unreflect(method);
                            handlerMap.put(handler.type(), (Handler) LambdaMetafactory
                                    .metafactory(caller, "handle",
                                            MethodType.methodType(Handler.class),
                                            mh.type(), mh, mh.type())
                                    .getTarget().invokeExact());
                        } else {
                            // Instance method, so we need to capture the type.
                            MethodType mt = MethodType
                                    .methodType(method.getReturnType(), method.getParameterTypes());
                            MethodHandle mh = caller.findVirtual(o.getClass(), method.getName(), mt);
                            MethodType mtGeneric = mh.type()
                                    .changeReturnType(Object.class);
                            handlerMap.put(handler.type(), (Handler) LambdaMetafactory
                                    .metafactory(caller, "handle",
                                            MethodType.methodType(Handler.class,
                                                    o.getClass()),
                                            mtGeneric.dropParameterTypes(0, 1), mh, mh.type()
                                                    .dropParameterTypes(0, 1))
                                    .getTarget().bindTo(o).invoke());
                        }
                    } catch (Throwable e) {
                        throw new RuntimeException("Exception while generating ClientResponseHandler", e);
                    }
                });

        return this;
    }

    public ClientResponseHandler generateErrorHandlers(@NonNull final MethodHandles.Lookup caller,
                                                       @NonNull final Object o) {
        Arrays.stream(o.getClass().getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(ServerErrorsHandler.class))
                .forEach(method -> {
                    ServerErrorsHandler handler = method.getAnnotation(ServerErrorsHandler.class);
                    if (!method.getParameterTypes()[0]
                            .isAssignableFrom(ResponseMsg.class)) {
                        throw new RuntimeException("Incorrect message type, expected "
                                + ResponseMsg.class + " but provided "
                                + method.getParameterTypes()[0]);
                    }

                    if (errorHandlerMap.containsKey(handler.type())) {
                        throw new RuntimeException("Handler for " + handler.type()
                                + " already registered!");
                    }

                    // convert the method into a Java8 Lambda for maximum execution speed...
                    try {
                        if (Modifier.isStatic(method.getModifiers())) {
                            MethodHandle mh = caller.unreflect(method);
                            errorHandlerMap.put(handler.type(), (Handler) LambdaMetafactory
                                    .metafactory(caller, "handle",
                                            MethodType.methodType(Handler.class),
                                            mh.type(), mh, mh.type())
                                    .getTarget().invokeExact());
                        } else {
                            // instance method, so we need to capture the type.
                            MethodType mt = MethodType
                                    .methodType(method.getReturnType(), method.getParameterTypes());
                            MethodHandle mh = caller.findVirtual(o.getClass(), method.getName(), mt);
                            MethodType mtGeneric = mh.type()
                                    .changeReturnType(Object.class);
                            errorHandlerMap.put(handler.type(), (Handler) LambdaMetafactory
                                    .metafactory(caller, "handle",
                                            MethodType.methodType(Handler.class,
                                                    o.getClass()),
                                            mtGeneric.dropParameterTypes(0, 1), mh, mh.type()
                                                    .dropParameterTypes(0, 1))
                                    .getTarget().bindTo(o).invoke());
                        }
                    } catch (Throwable e) {
                        throw new RuntimeException("Exception while generating ClientResponseHandler", e);
                    }
                });

        return this;
    }

    /**
     * Returns a set of payload cases that the client handles.
     *
     * @return The set of payload cases this client handles.
     */
    public Set<PayloadCase> getHandledCases() {
        return handlerMap.keySet();
    }

    /**
     * Returns a set of error cases that the client handles.
     *
     * @return The set of error cases this client handles.
     */
    public Set<ErrorCase> getHandledErrors() {
        return errorHandlerMap.keySet();
    }
}

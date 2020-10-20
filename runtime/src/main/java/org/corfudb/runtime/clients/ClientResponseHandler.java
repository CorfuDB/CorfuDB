package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;

import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Invokes the message handler to handle responses from server.
 *
 * This is for Protobuf Messages, for old CorfuMsg {@link ClientMsgHandler}.
 */
@Slf4j
public class ClientResponseHandler {

    @FunctionalInterface
    public interface Handler {
        Object handle(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) throws Exception;
    }

    /**
     * The handler map.
     */
    private final Map<PayloadCase, Handler> handlerMap;

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
    }

    /**
     * Add a handler to this ClientResponseHandler.
     *
     * @param payloadCase Response payload type.
     * @param handler The handler itself.
     * @return This handler, to support chaining.
     */
    public ClientResponseHandler addHandler(PayloadCase payloadCase,
                                            ClientResponseHandler.Handler handler) {
        handlerMap.put(payloadCase, handler);
        return this;
    }

    /**
     * Handle an incoming Response from server.
     *
     * @param response The Response to handle.
     * @param ctx The channel handler context.
     * @return True if the message was handled successfully.
     */
    public boolean handle(ResponseMsg response, ChannelHandlerContext ctx) {
        PayloadCase payloadCase = response.getPayload().getPayloadCase();
        IClientRouter router = client.getRouter();
        long requestId = response.getHeader().getRequestId();

        if (handlerMap.containsKey(payloadCase)) {
            try {
                Object ret = handlerMap.get(payloadCase)
                        .handle(response, ctx, router);
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
        // TODO
        Arrays.stream(o.getClass().getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(ResponseHandler.class))
                .forEach(method -> {
                    ResponseHandler handler = method.getAnnotation(ResponseHandler.class);
                    // TODO: this if clause could be avoided?
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
                    // convert the method into a Java8 Lambda for maximum execution speed...
                    try {
                        if (Modifier.isStatic(method.getModifiers())) {
                            MethodHandle mh = caller.unreflect(method);
                            MethodType mt = mh.type().changeParameterType(0, ResponseMsg.class);
                            handlerMap.put(handler.type(), (ClientResponseHandler.Handler) LambdaMetafactory
                                    .metafactory(caller, "handle",
                                            MethodType.methodType(ClientResponseHandler.Handler.class),
                                            mt, mh, mh.type())
                                    .getTarget().invokeExact());
                        }
                    } catch (Throwable e) {
                        log.error("Exception during incoming message handling", e);
                        throw new RuntimeException(e);
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
}

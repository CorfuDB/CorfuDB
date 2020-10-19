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
                .filter(x -> x.isAnnotationPresent(ResponseHandler.class))
                .forEach(x -> {
                    ResponseHandler a = x.getAnnotation(ResponseHandler.class);
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
     * Returns a set of payload cases that the client handles.
     *
     * @return The set of payload cases this client handles.
     */
    public Set<PayloadCase> getHandledCases() {
        return handlerMap.keySet();
    }
}

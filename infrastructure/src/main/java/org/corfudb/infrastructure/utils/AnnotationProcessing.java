package org.corfudb.infrastructure.utils;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.HandlerMethods;
import org.corfudb.infrastructure.ServerHandler;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;

import javax.annotation.Nonnull;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;

/**
 * Helper methods to extract server handler method mappings (i.e. message type -> method)
 *
 * <p>Created by Maithem on 12/5/19.
 */

@Slf4j
public class AnnotationProcessing {


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
                .forEach(method -> registerMethod(handler, caller, server, method));
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
    private static void registerMethod(@Nonnull HandlerMethods handler,
                                       @Nonnull final MethodHandles.Lookup caller,
                                       @Nonnull final AbstractServer server,
                                       @Nonnull final Method method) {
        final ServerHandler annotation = method.getAnnotation(ServerHandler.class);

        if (!method.getParameterTypes()[0].isAssignableFrom(annotation.type().messageType
                .getRawType())) {
            throw new UnrecoverableCorfuError("Incorrect message type, expected "
                    + annotation.type().messageType.getRawType() + " but provided "
                    + method.getParameterTypes()[0]);
        }
        if (handler.getHandledTypes().contains(annotation.type())) {
            throw new UnrecoverableCorfuError("HandlerMethod for " + annotation.type()
                    + " already registered!");
        }
        // convert the method into a Java8 Lambda for maximum execution speed...
        try {
            HandlerMethods.HandlerMethod<CorfuMsg> h;
            if (Modifier.isStatic(method.getModifiers())) {
                MethodHandle mh = caller.unreflect(method);
                MethodType mt = mh.type().changeParameterType(0, CorfuMsg.class);
                h = (HandlerMethods.HandlerMethod<CorfuMsg>) LambdaMetafactory.metafactory(caller,
                        "process", MethodType.methodType(HandlerMethods.HandlerMethod.class),
                        mt, mh, mh.type())
                        .getTarget().invokeExact();

            } else {
                // instance method, so we need to capture the type.
                MethodType mt = MethodType.methodType(method.getReturnType(),
                        method.getParameterTypes());
                MethodHandle mh = caller.findVirtual(server.getClass(), method.getName(), mt);
                MethodType mtGeneric = mh.type().changeParameterType(1, CorfuMsg.class);
                h = (HandlerMethods.HandlerMethod<CorfuMsg>) LambdaMetafactory.metafactory(caller,
                        "process",
                        MethodType.methodType(HandlerMethods.HandlerMethod.class, server.getClass()),
                        mtGeneric.dropParameterTypes(0, 1), mh,
                        mh.type().dropParameterTypes(0, 1)).getTarget()
                        .bindTo(server).invoke();
            }
            // Install pre-conditions on handler

            final HandlerMethods.HandlerMethod<CorfuMsg> genHandler = h::handle;

            // Install the handler in the map
            handler.addType(annotation.type(), genHandler);
        } catch (Throwable e) {
            log.error("Exception during message handler registration", e);
            throw new UnrecoverableCorfuError(e);

        }
    }
}

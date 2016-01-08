package org.corfudb.runtime.object;

import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;
import net.bytebuddy.matcher.ElementMatchers;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg;
import org.corfudb.runtime.exceptions.UnprocessedException;
import org.corfudb.runtime.view.StreamView;

import java.lang.reflect.*;
import java.util.Arrays;
import java.util.concurrent.Callable;

/**
 * Created by mwei on 1/7/16.
 */
@Slf4j
public class CorfuSMRObjectProxy {

    StreamView sv;
    Object smrObject;
    Object[] creationArguments;
    Class<?> originalClass;

    public CorfuSMRObjectProxy(StreamView sv, Class<?> originalClass) {
        this.sv = sv;
        this.originalClass = originalClass;
    }

    public static <T> Class<? extends T> getProxyClass(Class<T> type, StreamView sv) {
        CorfuSMRObjectProxy proxy = new CorfuSMRObjectProxy(sv, type);
        return new ByteBuddy()
                .subclass(type)
                .method(ElementMatchers.named("getSMRObject"))
                .intercept(MethodDelegation.to(proxy.getSMRObjectInterceptor()))
                .method(ElementMatchers.isAnnotatedWith(Mutator.class))
                .intercept(MethodDelegation.to(proxy.getMutatorInterceptor()))
                .method(ElementMatchers.isAnnotatedWith(Accessor.class))
                .intercept(MethodDelegation.to(proxy.getAccessorInterceptor()))
                .method(ElementMatchers.isAnnotatedWith(MutatorAccessor.class))
                .intercept(MethodDelegation.to(proxy.getAccessorInterceptor()))
                .make()
                .load(ClassLoader.getSystemClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
                .getLoaded();
    }

    public static <T> T getProxy(Class<T> type, StreamView sv) {
        try {
            return getProxyClass(type, sv).newInstance();
        } catch (InstantiationException | IllegalAccessException ie) {
            throw new RuntimeException("Unexpected exception opening object", ie);
        }
    }

    SMRObjectInterceptor getSMRObjectInterceptor() { return new SMRObjectInterceptor(); }
    AccessorInterceptor getAccessorInterceptor() { return new AccessorInterceptor(); }
    MutatorInterceptor getMutatorInterceptor() { return new MutatorInterceptor(); }
    //MutatorAccessorInterceptor getMutatorAccessorInterceptor() { return new MutatorAccessorInterceptor(); }

    public class SMRObjectInterceptor {
        @SuppressWarnings("unchecked")
        @RuntimeType
        public Object interceptGetSMRObject(@This ICorfuSMRObject obj) throws Exception {
            if (smrObject == null) {
                // We don't have an SMR object yet, so we'll need to create one.
                log.trace("SMR object not yet set, generating one.");
                // Is initialObject implemented? If it is, we use that implementation.
                try {
                    smrObject = obj.initialObject(creationArguments);
                    log.trace("SMR object generated using initial object.");
                } catch (UnprocessedException ue) {
                    //it is not, so we search the type hierarchy and call a default constructor.
                    Type[] ptA =  originalClass.getGenericInterfaces();
                    ParameterizedType pt = null;
                    for (Type p : ptA)
                    {
                        if (((ParameterizedType)p).getRawType().toString()
                                .endsWith("org.corfudb.runtime.object.ICorfuSMRObject"))
                        {
                            pt = (ParameterizedType) p;
                        }
                    }

                    log.trace("Determined SMR type to be {}", pt.getActualTypeArguments()[0].getTypeName());
                    if (creationArguments == null) {
                        smrObject = ((Class) ((ParameterizedType)pt.getActualTypeArguments()[0]).getRawType()
                                                                ).newInstance();
                    } else {
                        Class[] typeList = Arrays.stream(creationArguments)
                                .map(x -> x.getClass())
                                .toArray(s -> new Class[s]);

                        smrObject = ((Class)((ParameterizedType)pt.getActualTypeArguments()[0]).getRawType())
                                .getDeclaredConstructor(typeList)
                                .newInstance(creationArguments);
                    }
                }
            }
            return smrObject;
        }
    }

    public class MutatorInterceptor {

        /**
         * In the case of a pure mutator, we don't even need to ever call the underlying
         * mutator, since it will be applied during the upcall.
         * @param method        The method which was called.
         * @return              The return value (which should be void).
         * @throws Exception
         */
        @RuntimeType
        public Object interceptMutator(@Origin Method method) throws Exception {
            log.trace("Enter mutator: {}", method.getName());
            return null;
        }
    }

    public class AccessorInterceptor {
        @RuntimeType
        public Object interceptAccessor(@SuperCall Callable superMethod,
                                        @Origin Method method) throws Exception {
            log.trace("Enter accessor: {}", method.getName());
            // Linearize this access with respect to other accesses in the system.
            sync();
            // Now we can safely call the accessor.
            Object result = superMethod.call();
            log.trace("Exit accessor: {}", method.getName());
            return result;
        }
    }

    void applyUpdate(LogUnitReadResponseMsg.ReadResult m) {

    }

    synchronized public void sync() {
        Arrays.stream(sv.linearizedRead())
                .filter(m -> m.getResultType() == LogUnitReadResponseMsg.ReadResultType.DATA)
                .forEach(this::applyUpdate);
    }
}

package org.corfudb.runtime.objects;

import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.NamingStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.SuperMethodCall;
import net.bytebuddy.implementation.bind.annotation.*;
import org.corfudb.runtime.smr.ICorfuDBObject;
import org.corfudb.runtime.smr.smrprotocol.MethodTokenSMRCommand;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.view.ICorfuDBInstance;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import static net.bytebuddy.matcher.ElementMatchers.isAnnotatedWith;
import static net.bytebuddy.matcher.ElementMatchers.named;
import net.bytebuddy.implementation.FixedValue;

/**
 * Created by mwei on 9/30/15.
 */
@Slf4j
public class CorfuObjectByteBuddyProxy implements CorfuObjectProxy {

    static CorfuObjectByteBuddyProxy proxy = new CorfuObjectByteBuddyProxy();
    public static CorfuObjectByteBuddyProxy getProxy() {return proxy;}

    /**
     * Get the type of the underlying object
     */
    Class<?> getStateType(Class<?> objClass) {
        for (Type t : objClass.getGenericInterfaces())
        {
            if (t instanceof ParameterizedType && ((ParameterizedType)t).getRawType().equals(ICorfuDBObject.class))
            {
                ParameterizedType p = (ParameterizedType) t;
                Type r = p.getActualTypeArguments()[0];
                if (r instanceof ParameterizedType)
                {
                    return (Class<?>) ((ParameterizedType)r).getRawType();
                }
                return (Class<?>) r;
            }
        }
        throw new RuntimeException("Couldn't resolve underlying type!");
    }

    public static String getShortMethodName(Method m)
    {
        String longName = m.toString();
        int packageIndex = longName.substring(0, longName.indexOf("(")).lastIndexOf(".");
        return longName.substring(packageIndex + 1);
    }

    public static String getAccessorShortMethodNameOrEmpty(Method m)
    {
        String shortName = getShortMethodName(m);
        if (shortName.contains("$accessor$"))
        {
            //trim off appended name
            int decorationIndex = shortName.indexOf("$");
            int parenIndex = shortName.indexOf("(");
            return shortName.substring(0, decorationIndex) + shortName.substring(parenIndex);
        }
        return "";
    }

    public class CorfuObjectByteBuddyInterceptor {
        public @RuntimeType Object getState(@This ICorfuDBObject obj)
        {
            return obj.getSMREngine().getObject();
        }
    }

    public class CorfuObjectMutatorAccessorInterceptor {
        @SuppressWarnings("unchecked")
        public @RuntimeType Object mutatorAccessor( @This ICorfuDBObject obj,
                                                    @SuperCall Callable<Object> originalCall,
                                                    @AllArguments Object[] allArguments,
                                                    @Origin Method method)
                throws Exception
        {
            CompletableFuture<Object> cf = new CompletableFuture<>();
            obj.getSMREngine().proposeAsync(new MethodTokenSMRCommand<>(getShortMethodName(method), allArguments), cf, false)
                              .thenAccept(x -> obj.getSMREngine().sync((ITimestamp) x));
            return cf.join();
        }
    }

    public class CorfuObjectAccessorInterceptor {
        public @RuntimeType Object Accessor( @This ICorfuDBObject obj,
                                             @SuperCall Callable<Object> originalCall,
                                             @AllArguments Object[] allArguments,
                                             @Origin Method method)
                throws Exception
        {
            obj.getSMREngine().sync(null);
            return originalCall.call();
        }
    }

    public class CorfuObjectMutatorInterceptor {
        public @RuntimeType Object Mutator( @SuperCall Callable<Object> originalCall,
                                             @AllArguments Object[] allArguments,
                                             @Origin Method method)
                throws Exception
        {
            log.info("mutator");
            return originalCall.call();
        }
    }


    @Override
    public Class<?> getType(Class<?> corfuObjectClass, ICorfuDBInstance instance, UUID id) {
        try {
            return new ByteBuddy()
                    .subclass(corfuObjectClass)
                        // Dynamically generate base methods.
                        .method(named("getStreamID")).intercept(FixedValue.value(id))
                        .method(named("getInstance")).intercept(FixedValue.value(instance))
                    .method(named("getUnderlyingType")).intercept(FixedValue.value(getStateType(corfuObjectClass)))
                        // Redirected methods.
                    .method(named("getState"))
                        .intercept(MethodDelegation.to(new CorfuObjectByteBuddyInterceptor()))
                    .method(isAnnotatedWith(MutatorAccessor.class))
                        .intercept(MethodDelegation.to(new CorfuObjectMutatorAccessorInterceptor()))
                    .method(isAnnotatedWith(Accessor.class))
                        .intercept(MethodDelegation.to(new CorfuObjectAccessorInterceptor()))
                    .method(isAnnotatedWith(Mutator.class))
                    .intercept(MethodDelegation.to(new CorfuObjectMutatorInterceptor()))
                    .make()
                    .load(getClass().getClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
                    .getLoaded();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}

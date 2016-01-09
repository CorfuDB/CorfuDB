package org.corfudb.runtime.object;

import com.google.common.reflect.Reflection;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.bytecode.annotation.NoSuchClassError;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.method.ParameterDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.attribute.AnnotationAppender;
import net.bytebuddy.implementation.attribute.MethodAttributeAppender;
import net.bytebuddy.implementation.attribute.TypeAttributeAppender;
import net.bytebuddy.implementation.bind.annotation.*;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.matcher.ElementMatchers;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.UnprocessedException;
import org.corfudb.runtime.view.StreamView;
import org.corfudb.util.serializer.Serializers;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Created by mwei on 1/7/16.
 */
@Slf4j
public class CorfuSMRObjectProxy<P> {

    StreamView sv;
    Object smrObject;
    Object[] creationArguments;
    Class<?> originalClass;
    Class<?> generatedClass;

    public CorfuSMRObjectProxy(StreamView sv, Class<?> originalClass) {
        this.sv = sv;
        this.originalClass = originalClass;
    }

    public static <T> Class<? extends T> getProxyClass(CorfuSMRObjectProxy proxy, Class<T> type) {
        Class<? extends T> generatedClass = new ByteBuddy()
                .subclass(type)
                .method(ElementMatchers.named("getSMRObject"))
                .intercept(MethodDelegation.to(proxy.getSMRObjectInterceptor()))
                .method(ElementMatchers.isAnnotatedWith(Mutator.class))
                .intercept(MethodDelegation.to(proxy.getMutatorInterceptor()))
                        .method(ElementMatchers.isAnnotatedWith(Accessor.class))
                        .intercept(MethodDelegation.to(proxy.getAccessorInterceptor()))
                        .method(ElementMatchers.isAnnotatedWith(MutatorAccessor.class))
                        .intercept(MethodDelegation.to(proxy.getMutatorAccessorInterceptor()))
                        .make()
                        .load(ClassLoader.getSystemClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
                        .getLoaded();
        proxy.generatedClass = generatedClass;
        return generatedClass;
    }

    public static <T> T getProxy(Class<T> type, StreamView sv) {
        try {
            CorfuSMRObjectProxy<T> proxy = new CorfuSMRObjectProxy<>(sv, type);
            return getProxyClass(proxy, type).newInstance();
        } catch (InstantiationException | IllegalAccessException ie) {
            throw new RuntimeException("Unexpected exception opening object", ie);
        }
    }

    @Getter(lazy=true)
    private final SMRObjectInterceptor SMRObjectInterceptor = new SMRObjectInterceptor();

    @Getter(lazy=true)
    @SuppressWarnings("unchecked")
    private final AccessorInterceptor AccessorInterceptor = new AccessorInterceptor();

    @Getter(lazy=true)
    private final MutatorInterceptor MutatorInterceptor = new MutatorInterceptor();

    @Getter(lazy=true)
    private final MutatorAccessorInterceptor MutatorAccessorInterceptor = new MutatorAccessorInterceptor();

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
        public Object interceptMutator(@Origin String method,
                                       @AllArguments Object[] allArguments,
                                       @SuperCall Callable superMethod
        ) throws Exception {
            String name = new Exception().getStackTrace()[6].getClassName();
                    if (name.equals("org.corfudb.runtime.object.CorfuSMRObjectProxy"))
                    {
                        superMethod.call();
                    }
                    else {
                        writeUpdate(getShortMethodName(method), allArguments);
                    }
            return null;
        }
    }

    public class MutatorAccessorInterceptor {

        Object lastResult = null;

        @RuntimeType
        public Object interceptMutatorAccessor(
                                        @Origin String method,
                                        @SuperCall Callable superMethod,
                                        @AllArguments Object[] allArguments,
                                        @This P obj) throws Exception {
            String name = new Exception().getStackTrace()[6].getClassName();
            if (name.equals("org.corfudb.runtime.object.CorfuSMRObjectProxy"))
            {
                lastResult = superMethod.call();
                return lastResult;
            }
            else {
                // write the update to the stream
                long updatePos = writeUpdate(getShortMethodName(method), allArguments);
                // read up to this update.
                sync(obj, updatePos);
                // Now we can safely call the accessor.
                return lastResult;
            }
        }
    }

    public class AccessorInterceptor {

        @RuntimeType
        public Object interceptAccessor(@SuperCall Callable superMethod,
                                        @This P obj) throws Exception {
            // Linearize this access with respect to other accesses in the system.
            sync(obj, Long.MAX_VALUE);
            // Now we can safely call the accessor.
            Object result = superMethod.call();
            return result;
        }
    }

    public static String getShortMethodName(String longName)
    {
        int packageIndex = longName.substring(0, longName.indexOf("(")).lastIndexOf(".");
        return longName.substring(packageIndex + 1);
    }

    public static String getMethodNameOnlyFromString(String s) {
        return s.substring(0, s.indexOf("("));
    }

    public static Class[] getArgumentTypesFromString(String s) {
        String argList = s.substring(s.indexOf("(") + 1, s.length() - 1);
        return Arrays.stream(argList.split(","))
                .filter(x -> !x.equals(""))
                .map(x -> {
                    try {
                        return Class.forName(x);
                    } catch (ClassNotFoundException cnfe) {
                        log.warn("Class {} not found", x);
                        return null;
                    }
                })
                .toArray(Class[]::new);
    }

    long writeUpdate(String method, Object[] arguments)
    {
        log.trace("Write update: {} with arguments {}", getShortMethodName(method), arguments);
        return sv.write(new SMREntry(getShortMethodName(method), arguments, Serializers.SerializerType.JAVA));
    }

    void applyUpdate(SMREntry entry, P obj) {
        log.trace("Apply update: {}", entry);
        // Look for the uninstrumented method
        try {
            Method m = obj.getClass().getMethod(getMethodNameOnlyFromString(entry.getSMRMethod()),
                    getArgumentTypesFromString(entry.getSMRMethod()));
            m.invoke(obj, entry.getSMRArguments());
        } catch (NoSuchMethodException n)
        {
            log.error("Couldn't find method {} during apply update", entry.getSMRMethod(), n);
        }
        catch (InvocationTargetException | IllegalAccessException iae)
        {
            log.error("Couldn't dispatch method {} during apply update", entry.getSMRMethod(), iae);
        }
    }

    synchronized public void sync(P obj, long maxPos) {
        Arrays.stream(sv.readTo(maxPos))
                .filter(m -> m.getResultType() == LogUnitReadResponseMsg.ReadResultType.DATA)
                .map(LogUnitReadResponseMsg.ReadResult::getPayload)
                .filter(m -> m instanceof SMREntry)
                .forEach(m -> applyUpdate((SMREntry)m, obj));
    }
}

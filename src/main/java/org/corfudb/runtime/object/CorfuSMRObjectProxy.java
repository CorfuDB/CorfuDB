package org.corfudb.runtime.object;

import com.google.common.reflect.Reflection;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.bytecode.annotation.NoSuchClassError;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
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
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.logprotocol.TXEntry;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.UnprocessedException;
import org.corfudb.runtime.view.AbstractReplicationView;
import org.corfudb.runtime.view.StreamView;
import org.corfudb.util.serializer.Serializers;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.*;
import java.util.*;
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
    Serializers.SerializerType serializer;
    CorfuRuntime runtime;
    long timestamp;

    @Getter
    boolean isCorfuObject = false;

    public CorfuSMRObjectProxy(CorfuRuntime runtime, StreamView sv, Class<?> originalClass) {
        this.runtime = runtime;
        this.sv = sv;
        this.originalClass = originalClass;
        this.serializer = Serializers.SerializerType.JAVA;
        this.timestamp = -1L;
        if (Arrays.stream(originalClass.getInterfaces()).anyMatch(ICorfuSMRObject.class::isAssignableFrom)) {
            isCorfuObject = true;
        }
    }

    public static <T,R extends ISMRInterface> Class<? extends T>
    getProxyClass(CorfuSMRObjectProxy proxy, Class<T> type, Class<R> overlay) {
        if (Arrays.stream(type.getInterfaces()).anyMatch(ICorfuSMRObject.class::isAssignableFrom))
        {
            log.trace("Detected ICorfuSMRObject({}), instrumenting methods.", type);
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
        else if (overlay != null) {
            log.trace("Detected Overlay({}), instrumenting methods", overlay);
        }
        else if (Arrays.stream(type.getInterfaces()).anyMatch(ISMRInterface.class::isAssignableFrom)){
            ISMRInterface[] iface = Arrays.stream(type.getInterfaces())
                    .filter(ISMRInterface.class::isAssignableFrom)
                    .toArray(ISMRInterface[]::new);
            log.trace("Detected ISMRInterfaces({}), instrumenting methods", iface);
        }
        else {
            log.trace("{} is not an ICorfuSMRObject, no ISMRInterfaces and no overlay provided. " +
                    "Instrumenting all methods as mutatorAccessors but respecting annotations", type);
            Class<? extends T> generatedClass = new ByteBuddy()
                    .subclass(type)
                    .method(ElementMatchers.isAnnotatedWith(Mutator.class))
                    .intercept(MethodDelegation.to(proxy.getMutatorInterceptor()))
                    .method(ElementMatchers.isAnnotatedWith(Accessor.class))
                    .intercept(MethodDelegation.to(proxy.getAccessorInterceptor()))
                    .method(ElementMatchers.isAnnotatedWith(MutatorAccessor.class))
                    .intercept(MethodDelegation.to(proxy.getMutatorAccessorInterceptor()))
                    .method(ElementMatchers.not(ElementMatchers.isAnnotatedWith(Mutator.class))
                                            .and(ElementMatchers.not(ElementMatchers.isAnnotatedWith(MutatorAccessor.class)))
                    )
                    .intercept(MethodDelegation.to(proxy.getMutatorAccessorInterceptor()))
                    .make()
                    .load(ClassLoader.getSystemClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
                    .getLoaded();
            proxy.generatedClass = generatedClass;
            return generatedClass;
        }
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    public static <T,R extends ISMRInterface>
        T getProxy(@NonNull Class<T> type, Class<R> overlay, @NonNull StreamView sv, @NonNull CorfuRuntime runtime) {
        try {
            CorfuSMRObjectProxy<T> proxy = new CorfuSMRObjectProxy<>(runtime, sv, type);
            return getProxyClass(proxy, type, overlay).newInstance();
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
            if (obj == null && smrObject == null)
            {
                log.trace("SMR object not yet set, generating one.");
                if (creationArguments == null) {
                    smrObject = originalClass.newInstance();
                } else {
                    Class[] typeList = Arrays.stream(creationArguments)
                            .map(x -> x.getClass())
                            .toArray(s -> new Class[s]);

                    originalClass
                            .getDeclaredConstructor(typeList)
                            .newInstance(creationArguments);
                }
            }
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
            if (TransactionalContext.isInTransaction())
            {
                String name = new Exception().getStackTrace()[6].getMethodName();
                if (name.equals("interceptAccessor")) {
                    return TransactionalContext.getCurrentContext().getObjectRead(CorfuSMRObjectProxy.this);
                }
                else if (name.equals("interceptMutatorAccessor")){
                    return TransactionalContext.getCurrentContext().getObjectWrite(CorfuSMRObjectProxy.this);
                }
                else {
                    return TransactionalContext.getCurrentContext().getObjectReadWrite(CorfuSMRObjectProxy.this);
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
                                       @Origin Method Mmethod,
                                       @AllArguments Object[] allArguments,
                                       @SuperCall Callable superMethod
        ) throws Exception {
            StackTraceElement[] stack = new Exception().getStackTrace();
            if (stack.length > 6 && stack[6].getClassName().equals("org.corfudb.runtime.object.CorfuSMRObjectProxy"))
            {
                        if (isCorfuObject) {
                            return superMethod.call();
                        }
                        else {
                            return Mmethod.invoke(getSMRObjectInterceptor().interceptGetSMRObject(null));
                        }
                    }
                    else if (!TransactionalContext.isInTransaction()){
                        writeUpdate(getShortMethodName(method), allArguments);
                    }
                    else {
                        // in a transaction, we add the update to the TX buffer and apply the update
                        // immediately.
                        TransactionalContext.getCurrentContext().bufferObjectUpdate(CorfuSMRObjectProxy.this,
                                getShortMethodName(method), allArguments, serializer);
                    }
            return null;
        }
    }

    public class MutatorAccessorInterceptor {

        Object lastResult = null;

        @RuntimeType
        public Object interceptMutatorAccessor(
                                        @Origin String method,
                                        @Origin Method Mmethod,
                                        @SuperCall Callable superMethod,
                                        @AllArguments Object[] allArguments,
                                        @This P obj) throws Exception {
            StackTraceElement[] stack = new Exception().getStackTrace();
            if (stack.length > 6 && stack[6].getClassName().equals("org.corfudb.runtime.object.CorfuSMRObjectProxy"))
            {
                lastResult = superMethod.call();
                return lastResult;
            }
            else if (!TransactionalContext.isInTransaction()){
                // write the update to the stream
                long updatePos = writeUpdate(getShortMethodName(method), allArguments);
                // read up to this update.
                sync(obj, updatePos);
                // Now we can safely call the accessor.
                return lastResult;
            } else {
                // in a transaction, we add the update to the TX buffer and apply the update
                // immediately.
                TransactionalContext.getCurrentContext().bufferObjectUpdate(CorfuSMRObjectProxy.this,
                        getShortMethodName(method), allArguments, serializer);
                if (isCorfuObject) {
                    return superMethod.call();
                }
                else {
                    return Mmethod.invoke(getSMRObjectInterceptor().interceptGetSMRObject(null));
                }
            }
        }
    }

    public class AccessorInterceptor {

        @RuntimeType
        public Object interceptAccessor(@SuperCall Callable superMethod,
                                        @Origin Method method,
                                        @This P obj) throws Exception {
            // Linearize this access with respect to other accesses in the system.
            if (!TransactionalContext.isInTransaction()) {
                sync(obj, Long.MAX_VALUE);
            }
            // Now we can safely call the accessor.
            if (isCorfuObject) {
                return superMethod.call();
            }
            else {
                return method.invoke(getSMRObjectInterceptor().interceptGetSMRObject(null));
            }
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
        return sv.write(new SMREntry(getShortMethodName(method), arguments, serializer));
    }

    boolean applySMRUpdate(long address, SMREntry entry, P obj)
    {
        log.trace("Apply SMR update: {}", entry);
        // Look for the uninstrumented method
        try {
            Method m = obj.getClass().getMethod(getMethodNameOnlyFromString(entry.getSMRMethod()),
                    getArgumentTypesFromString(entry.getSMRMethod()));
            // Execute the SMR command
            m.invoke(obj, entry.getSMRArguments());
            // Update the current timestamp.
            timestamp = address;
            return true;
        } catch (NoSuchMethodException n) {
            log.error("Couldn't find method {} during apply update", entry.getSMRMethod(), n);
        } catch (InvocationTargetException | IllegalAccessException iae) {
            log.error("Couldn't dispatch method {} during apply update", entry.getSMRMethod(), iae);
        }
        return false;
    }

    boolean applyUpdate(long address, LogEntry entry, P obj) {
        if (entry instanceof SMREntry) {
            return applySMRUpdate(address, (SMREntry)entry, obj);
        } else if (entry instanceof TXEntry)
        {
            TXEntry txEntry = (TXEntry) entry;
            log.trace("Apply TX update: {}", txEntry);
            // First, determine if the TX is abort.
            // Use backpointers if we have them.
            if (txEntry.isAborted(runtime, address)){
                return false;
            }

            // The TX has committed, apply updates for this object.
            txEntry.getTxMap().get(sv.getStreamID())
                    .getUpdates().stream()
                    .forEach(x -> applySMRUpdate(address, x, obj));

            return true;
        }
        return false;
    }

    synchronized public void sync(P obj, long maxPos) {
        Arrays.stream(sv.readTo(maxPos))
                .filter(m -> m.getResult().getResultType() == LogUnitReadResponseMsg.ReadResultType.DATA)
                .filter(m -> m.getResult().getPayload() instanceof SMREntry ||
                        m.getResult().getPayload() instanceof TXEntry)
                .forEach(m -> applyUpdate(m.getAddress(), (LogEntry) m.getResult().getPayload(), obj));
    }
}

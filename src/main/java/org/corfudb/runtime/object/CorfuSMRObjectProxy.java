package org.corfudb.runtime.object;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.Reflection;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.bytecode.annotation.NoSuchClassError;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.method.ParameterDescription;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.ModifierContributor;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
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
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.UnprocessedException;
import org.corfudb.runtime.view.AbstractReplicationView;
import org.corfudb.runtime.view.StreamView;
import org.corfudb.util.serializer.Serializers;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by mwei on 1/7/16.
 */
@Slf4j
public class CorfuSMRObjectProxy<P> {

    @Getter
    StreamView sv;
    Object smrObject;
    Object[] creationArguments;
    @Getter
    Class<P> originalClass;
    Class<?> generatedClass;
    Map<Long, CompletableFuture<Object>> completableFutureMap;
    @Getter
    Serializers.SerializerType serializer;
    @Getter
    CorfuRuntime runtime;
    @Getter
    Map<String, Method> methodHashTable;

    @Getter
    long timestamp;

    @Getter
    boolean isCorfuObject = false;

    public UUID getStreamID() {
        return sv.getStreamID();
    }

    final static AnnotationDescription instrumentedDescription =
             AnnotationDescription.Builder.ofType(Instrumented.class)
            .build();

    final static AnnotationDescription instrumentedObjectDescription =
            AnnotationDescription.Builder.ofType(InstrumentedCorfuObject.class)
                    .build();


    public CorfuSMRObjectProxy(CorfuRuntime runtime, StreamView sv, Class<P> originalClass, Serializers.SerializerType serializer) {
        this.runtime = runtime;
        this.sv = sv;
        this.originalClass = originalClass;
        this.completableFutureMap = new ConcurrentHashMap<>();
        this.serializer = serializer;
        this.timestamp = -1L;
        if (Arrays.stream(originalClass.getInterfaces()).anyMatch(ICorfuSMRObject.class::isAssignableFrom)) {
            isCorfuObject = true;
        }
        this.methodHashTable = new ConcurrentHashMap<>();
    }

    @SuppressWarnings("unchecked")
    public Method findCorrespondingProxyMethod(Method m, Class proxyClass)
    {
        try {
            return proxyClass.getDeclaredMethod(m.getName(), m.getParameterTypes());
        } catch (NoSuchMethodException nsme)
        {
            log.warn("Couldn't find corresponding proxy method for method {}", m.getName());
            throw new RuntimeException("Inconsistent state resolving proxy!");
        }
    }

    public void calculateMethodHashTable(Class proxyClass) {
        Arrays.stream(originalClass.getDeclaredMethods())
                .forEach(x -> {
                    for (Annotation a : x.getDeclaredAnnotations())
                    {
                        if (a instanceof Mutator)
                        {
                            methodHashTable.put(((Mutator)a).name(), findCorrespondingProxyMethod(x, proxyClass));
                        }
                        else if (a instanceof MutatorAccessor)
                        {
                            methodHashTable.put(((MutatorAccessor)a).name(), findCorrespondingProxyMethod(x, proxyClass));
                        }
                    }
                });
    }

    public static <T,R extends ISMRInterface> Class<? extends T>
    getProxyClass(CorfuSMRObjectProxy proxy, Class<T> type, Class<R> overlay) {
        if (Arrays.stream(type.getInterfaces()).anyMatch(ICorfuSMRObject.class::isAssignableFrom))
        {
            log.trace("Detected ICorfuSMRObject({}), instrumenting methods.", type);
            Class<? extends T> generatedClass = new ByteBuddy()
                    .subclass(type)
                   // .defineField("_corfuStreamID", UUID.class, FieldManifestation.PLAIN)
                    .defineField("_corfuSMRProxy", CorfuSMRObjectProxy.class)
                    .defineField("_corfuStreamID", UUID.class)
                    .method(ElementMatchers.named("getSMRObject"))
                    .intercept(MethodDelegation.to(proxy.getSMRObjectInterceptor()))
                    .method(ElementMatchers.named("getRuntime"))
                    .intercept(MethodDelegation.to(proxy, "getRuntime"))
                    .method(ElementMatchers.named("getStreamID"))
                    .intercept(MethodDelegation.to(proxy, "getStreamID"))
                    .method(ElementMatchers.isAnnotatedWith(Mutator.class))
                    .intercept(MethodDelegation.to(proxy.getMutatorInterceptor()))
                    .method(ElementMatchers.isAnnotatedWith(Accessor.class))
                    .intercept(MethodDelegation.to(proxy.getAccessorInterceptor()))
                    .method(ElementMatchers.isAnnotatedWith(MutatorAccessor.class))
                    .intercept(MethodDelegation.to(proxy.getMutatorAccessorInterceptor()))
                    .make()
                    .load(CorfuSMRObjectProxy.class.getClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
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

            // dump all method annotations
            log.trace("All methods for {}:", type);
            if (log.isTraceEnabled()) {
                Method[] ms = type.getMethods();
                for (Method m : ms) {
                    log.trace("{}: {}", m.getName(), m.getAnnotations());
                }
            }
                    DynamicType.Builder<T> bb = new ByteBuddy().subclass(type)
                            .defineField("_corfuStreamID", UUID.class)
                            .defineField("_corfuSMRProxy", CorfuSMRObjectProxy.class)
                            .implement(ICorfuSMRObject.class)
                            .method(ElementMatchers.named("getSMRObject"))
                            .intercept(MethodDelegation.to(proxy.getSMRObjectInterceptor()))
                            .method(ElementMatchers.named("getStreamID"))
                            .intercept(MethodDelegation.to(proxy, "getStreamID"))
                            .method(ElementMatchers.named("getRuntime"))
                            .intercept(MethodDelegation.to(proxy, "getRuntime"));

                    try {
                                bb = bb.method(ElementMatchers.isAnnotatedWith(Mutator.class)
                                .and(ElementMatchers.not(ElementMatchers.isAnnotatedWith(Instrumented.class))))
                                                .intercept(MethodDelegation.to(proxy.getMutatorInterceptor()))
                                                .annotateMethod(instrumentedDescription);
                    } catch (NoSuchMethodError nsme) {
                        log.trace("Class {} has no mutators", type);
                    }
                    try {
                        bb = bb.method(ElementMatchers.isAnnotatedWith(Accessor.class)
                                .and(ElementMatchers.not(ElementMatchers.isAnnotatedWith(Instrumented.class))))
                                .intercept(MethodDelegation.to(proxy.getAccessorInterceptor()))
                                .annotateMethod(instrumentedDescription);
                    } catch (NoSuchMethodError nsme) {
                        log.trace("Class {} has no accessors", type);
                    }
                    try {
                        bb = bb.method(ElementMatchers.isAnnotatedWith(MutatorAccessor.class)
                                .and(ElementMatchers.not(ElementMatchers.isAnnotatedWith(Instrumented.class))))
                                .intercept(MethodDelegation.to(proxy.getMutatorAccessorInterceptor()))
                                .annotateMethod(instrumentedDescription);
                    } catch (NoSuchMethodError nsme)
                    {
                        log.trace("Class {} has no mutatoraccessors", type);
                    }

                    bb = bb.method(ElementMatchers.not(ElementMatchers.isAnnotatedWith(Mutator.class))
                            .and(ElementMatchers.not(ElementMatchers.isAnnotatedWith(Accessor.class)))
                            .and(ElementMatchers.not(ElementMatchers.isAnnotatedWith(MutatorAccessor.class)))
                            .and(ElementMatchers.not(ElementMatchers.isAnnotatedWith(DontInstrument.class)))
                            .and(ElementMatchers.not(ElementMatchers.isAnnotatedWith(Instrumented.class)))
                            .and(ElementMatchers.not(ElementMatchers.isDeclaredBy(Object.class)))
                            .and(ElementMatchers.not(ElementMatchers.isDefaultMethod())))
                            .intercept(MethodDelegation.to(proxy.getMutatorAccessorInterceptor()))
                            .annotateMethod(instrumentedDescription);

                    bb.annotateType(instrumentedObjectDescription);
            Class<? extends T> generatedClass =
                     bb.make().load(CorfuSMRObjectProxy.class.getClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
                    .getLoaded();
            proxy.generatedClass = generatedClass;
            return generatedClass;
        }
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    public static <T,R extends ISMRInterface>
        T getProxy(@NonNull Class<T> type, Class<R> overlay, @NonNull StreamView sv, @NonNull CorfuRuntime runtime,
                   Serializers.SerializerType serializer) {
        try {
            CorfuSMRObjectProxy<T> proxy = new CorfuSMRObjectProxy<>(runtime, sv, type, serializer);
            T ret = getProxyClass(proxy, type, overlay).newInstance();
            proxy.calculateMethodHashTable(ret.getClass());
            Field f = ret.getClass().getDeclaredField("_corfuStreamID");
            f.setAccessible(true);
            f.set(ret, sv.getStreamID());
            Field f2 = ret.getClass().getDeclaredField("_corfuSMRProxy");
            f2.setAccessible(true);
            f2.set(ret, proxy);
            return ret;
        } catch (InstantiationException | IllegalAccessException | NoSuchFieldException ie) {
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
                            .map(Object::getClass)
                            .toArray(Class[]::new);

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
                                .map(Object::getClass)
                                .toArray(Class[]::new);

                        smrObject = ((Class)((ParameterizedType)pt.getActualTypeArguments()[0]).getRawType())
                                .getDeclaredConstructor(typeList)
                                .newInstance(creationArguments);
                    }
                }
            }
            if (TransactionalContext.isInTransaction()
                    && !TransactionalContext.getCurrentContext().isInSyncMode())
            {
                String name = new Exception().getStackTrace()[6].getMethodName();
                if (name.equals("interceptAccessor")) {
                    return TransactionalContext.getCurrentContext().getObjectRead(CorfuSMRObjectProxy.this);
                }
                else if (name.equals("interceptMutator")){
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
         * @param Mmethod        The method which was called.
         * @return              The return value (which should be void).
         * @throws Exception
         */
        @RuntimeType
        public Object interceptMutator(@Origin Method Mmethod,
                                       @AllArguments Object[] allArguments,
                                       @SuperCall Callable superMethod
        ) throws Exception {
            String method = getSMRMethodName(Mmethod);
            log.trace("+Mutator {}", method);
            StackTraceElement[] stack = new Exception().getStackTrace();
            if (stack.length > 6 && stack[6].getClassName().equals("org.corfudb.runtime.object.CorfuSMRObjectProxy"))
            {
                        if (isCorfuObject) {
                            return superMethod.call();
                        }
                        else {
                            return Mmethod.invoke(getSMRObjectInterceptor().interceptGetSMRObject(null), allArguments);
                        }
                    }
                    else if (!TransactionalContext.isInTransaction()){
                        writeUpdate(method, allArguments);
                    }
                    else {
                        // in a transaction, we add the update to the TX buffer and apply the update
                        // immediately.
                        TransactionalContext.getCurrentContext().bufferObjectUpdate(CorfuSMRObjectProxy.this,
                                method, allArguments, serializer);
                    }
            return null;
        }
    }

    public class MutatorAccessorInterceptor {

        Object lastResult = null;

        @RuntimeType
        public Object interceptMutatorAccessor(
                                        @Origin Method Mmethod,
                                        @SuperCall Callable superMethod,
                                        @AllArguments Object[] allArguments,
                                        @This P obj) throws Exception {
            String method = getSMRMethodName(Mmethod);
            log.trace("+MutatorAccessor {}", method);

            StackTraceElement[] stack = new Exception().getStackTrace();
            if (stack.length > 6 && stack[6].getClassName().equals("org.corfudb.runtime.object.CorfuSMRObjectProxy"))
            {
                return doUnderlyingCall(superMethod, Mmethod, allArguments);
            }
            else if (!TransactionalContext.isInTransaction()){
                // write the update to the stream and map a future for the completion.
                long updatePos = writeUpdateAndMapFuture(method, allArguments);
                // read up to this update.
                sync(obj, updatePos);
                // Now we can safely wait on the accessor.
                Object ret = completableFutureMap.get(updatePos).join();
                completableFutureMap.remove(updatePos);
                return ret;
            } else {
                // If this is the first access in this transaction, we should sync it first.
                doTransactionalSync(obj);
                // in a transaction, we add the update to the TX buffer and apply the update
                // immediately.
                TransactionalContext.getCurrentContext().bufferObjectUpdate(CorfuSMRObjectProxy.this,
                        method, allArguments, serializer);
                return doUnderlyingCall(superMethod, Mmethod, allArguments);
            }
        }

        private synchronized Object doUnderlyingCall(Callable superMethod, Method method, Object[] arguments)
                throws Exception {
            if (isCorfuObject) {
                return superMethod.call();
            }
            else {
                return method.invoke(getSMRObjectInterceptor().interceptGetSMRObject(null), arguments);
            }
        }
    }

    public class AccessorInterceptor {

        @RuntimeType
        public Object interceptAccessor(@SuperCall Callable superMethod,
                                        @Origin Method method,
                                        @AllArguments Object[] arguments,
                                        @This P obj) throws Exception {
            log.trace("+Accessor {}", method);
            // Linearize this access with respect to other accesses in the system.
            if (!TransactionalContext.isInTransaction()) {
                sync(obj, Long.MAX_VALUE);
                return doUnderlyingCall(superMethod, method, arguments);
            }
            else {
                doTransactionalSync(obj);
                Object ret = doUnderlyingCall(superMethod, method, arguments);
                // If the object was written to (due to transactional clone), the read set is not resolvable.
                if (!TransactionalContext.getCurrentContext().isObjectCloned(CorfuSMRObjectProxy.this))
                {
                    // Store the read set with the context.
                    TransactionalContext.getCurrentContext().addReadSet(CorfuSMRObjectProxy.this,
                            getSMRMethodName(method), ret);
                }
                return ret;
            }
        }

        private synchronized Object doUnderlyingCall(Callable superMethod, Method method, Object[] arguments)
        throws Exception {
            if (isCorfuObject) {
                return superMethod.call();
            }
            else {
                return method.invoke(getSMRObjectInterceptor().interceptGetSMRObject(null), arguments);
            }
        }
    }

    public synchronized void doTransactionalSync(P obj) {
        TransactionalContext.getCurrentContext().setInSyncMode(true);
        // Otherwise we should make sure we're sync'd up to the TX
        sync(obj, TransactionalContext.getCurrentContext().getFirstReadTimestamp());
        TransactionalContext.getCurrentContext().setInSyncMode(false);
    }

    public static String getShortMethodName(String longName)
    {
        int packageIndex = longName.substring(0, longName.indexOf("(")).lastIndexOf(".");
        return longName.substring(packageIndex + 1);
    }

    public static String getMethodNameOnlyFromString(String s) {
        return s.substring(0, s.indexOf("("));
    }

    static Map<String, Class> primitiveTypeMap = ImmutableMap.<String, Class>builder()
            .put("int", Integer.TYPE)
            .put("long", Long.TYPE)
            .put("double", Double.TYPE)
            .put("float", Float.TYPE)
            .put("bool", Boolean.TYPE)
            .put("char", Character.TYPE)
            .put("byte", Byte.TYPE)
            .put("void", Void.TYPE)
            .put("short", Short.TYPE)
            .put("int[]", int[].class)
            .put("long[]", long[].class)
            .put("double[]", double[].class)
            .put("float[]", float[].class)
            .put("bool[]", boolean[].class)
            .put("char[]", char[].class)
            .put("byte[]", byte[].class)
            .put("short[]", short[].class)
            .build();

    public static Class<?> getPrimitiveType(String s) {
        return primitiveTypeMap.get(s);
    }

    public static Class[] getArgumentTypesFromString(String s) {
        String argList = s.substring(s.indexOf("(") + 1, s.length() - 1);
        return Arrays.stream(argList.split(","))
                .filter(x -> !x.equals(""))
                .map(x -> {
                    try {
                        return Class.forName(x);
                    } catch (ClassNotFoundException cnfe) {
                        Class retVal = getPrimitiveType(x);
                        if (retVal == null) {
                            log.warn("Class {} not found", x);
                        }
                        return retVal;
                    }
                })
                .toArray(Class[]::new);
    }

    public String getSMRMethodName(Method method) {
        for (Annotation a : method.getDeclaredAnnotations())
        {
            if (a instanceof Mutator)
            {
                if (!((Mutator) a).name().equals(""))
                {
                    return ((Mutator) a).name();
                }
            }
            else if (a instanceof MutatorAccessor)
            {
                if (!((MutatorAccessor) a).name().equals(""))
                {
                    return ((MutatorAccessor) a).name();
                }
            }
        }
        return getShortMethodName(method.toString());
    }

    long writeUpdate(String method, Object[] arguments)
    {
        log.trace("Write update: {} with arguments {}", method, arguments);
        return sv.write(new SMREntry(method, arguments, serializer));
    }

    long writeUpdateAndMapFuture(String method, Object[] arguments)
    {
        log.trace("Write update and map future: {} with arguments {}", method, arguments);
        return sv.acquireAndWrite(new SMREntry(method, arguments, serializer),
                t -> completableFutureMap.put(t, new CompletableFuture<>()),
               completableFutureMap::remove);
    }

    boolean applySMRUpdate(long address, SMREntry entry, P obj)
    {
        log.trace("Apply SMR update at {} : {}", address, entry);
        // Look for the uninstrumented method
        try {
            // Find the method by using the method name hash table.
            Method m = methodHashTable.computeIfAbsent(entry.getSMRMethod(),
                    s -> {
                        try {
                        return obj.getClass().getMethod(getMethodNameOnlyFromString(entry.getSMRMethod()),
                            getArgumentTypesFromString(entry.getSMRMethod()));
                        }
                        catch (NoSuchMethodException nsme)
                        {
                            return null;
                        }});

            if (m == null) { throw new NoSuchMethodException(entry.getSMRMethod()); }
            // Execute the SMR command
            Object ret = m.invoke(obj, entry.getSMRArguments());
            // Update the current timestamp.
            timestamp = address;
            log.trace("Timestamp for [{}] updated to {}",sv.getStreamID(), address);
            if (completableFutureMap.containsKey(address))
            {
                completableFutureMap.get(address).complete(ret);
            }
            return true;
        } catch (NoSuchMethodException n) {
            log.error("Couldn't find method {} during apply update", entry.getSMRMethod(), n);
            if (completableFutureMap.containsKey(address))
            {
                completableFutureMap.get(address).completeExceptionally(n);
            }
        } catch (InvocationTargetException | IllegalAccessException iae) {
            log.error("Couldn't dispatch method {} during apply update", entry.getSMRMethod(), iae);
            if (completableFutureMap.containsKey(address))
            {
                completableFutureMap.get(address).completeExceptionally(iae);
            }
        } catch (Exception e)
        {
            log.warn("Exception during application of SMR method {}", entry.getSMRMethod());
            if (completableFutureMap.containsKey(address))
            {
                completableFutureMap.get(address).completeExceptionally(e);
            }
        }
        return false;
    }

    boolean applyUpdate(long address, LogEntry entry, P obj) {
        if (entry instanceof SMREntry) {
            return applySMRUpdate(address, (SMREntry)entry, obj);
        } else if (entry instanceof TXEntry)
        {
            TXEntry txEntry = (TXEntry) entry;
            log.trace("Apply TX update at {}: {}", address, txEntry);
            // First, determine if the TX is abort.
            if (txEntry.isAborted()){
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
        log.trace("Object[{}] sync to pos {}", sv.getStreamID(), maxPos == Long.MAX_VALUE ? "MAX" : maxPos);
        Arrays.stream(sv.readTo(maxPos))
                .filter(m -> m.getResultType() == LogUnitReadResponseMsg.ReadResultType.DATA)
                .filter(m -> m.getPayload() instanceof SMREntry ||
                        m.getPayload() instanceof TXEntry)
                .forEach(m -> applyUpdate(m.getAddress(), (LogEntry) m.getPayload(), obj));
    }
}

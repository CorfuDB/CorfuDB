package org.corfudb.runtime.object;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;
import org.corfudb.annotations.Mutator;
import org.corfudb.annotations.MutatorAccessor;
import org.corfudb.protocols.logprotocol.*;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.UnprocessedException;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.StreamView;
import org.corfudb.util.LockUtils;
import org.corfudb.util.ReflectionUtils;
import org.corfudb.util.serializer.ISerializer;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by mwei on 1/7/16.
 */
@Slf4j
@Deprecated
public class CorfuSMRObjectProxy<P> extends CorfuObjectProxy<P> {

    P smrObject;
    @Setter
    Object underlyingObject;
    @Getter
    @Setter
    Object[] creationArguments = new Object[0];
    Map<Long, CompletableFuture<Object>> completableFutureMap;
    @Getter
    Map<String, Method> methodHashTable;
    @Getter
    ReadWriteLock rwLock;
    @Getter
    boolean isCorfuObject = false;
    @Setter
    Class stateClass;
    @Setter
    boolean selfState = false;
    ICorfuSMRObject.SMRHandlerMethod postHandler;

    public CorfuSMRObjectProxy(CorfuRuntime runtime, StreamView sv,
                               Class<P> originalClass, ISerializer serializer) {
        super(runtime, sv, originalClass, serializer);
        this.completableFutureMap = new ConcurrentHashMap<>();
        if (Arrays.stream(originalClass.getInterfaces()).anyMatch(ICorfuSMRObject.class::isAssignableFrom)) {
            isCorfuObject = true;
        }
        this.methodHashTable = new ConcurrentHashMap<>();
        this.rwLock = new ReentrantReadWriteLock();
    }

    public P getSmrObject() {
        if (selfState) {
            return (P) underlyingObject;
        }
        return smrObject;
    }

    @SuppressWarnings("unchecked")
    public Method findCorrespondingProxyMethod(Method m, Class proxyClass) {
        try {
            return proxyClass.getDeclaredMethod(m.getName(), m.getParameterTypes());
        } catch (NoSuchMethodException nsme) {
            log.warn("Couldn't find corresponding proxy method for method {}", m.getName());
            throw new RuntimeException("Inconsistent state resolving proxy!");
        }
    }

    public void calculateMethodHashTable(Class proxyClass) {
        Arrays.stream(originalClass.getDeclaredMethods())
                .forEach(x -> {
                    for (Annotation a : x.getDeclaredAnnotations()) {
                        if (a instanceof Mutator) {
                            methodHashTable.put(((Mutator) a).name(), findCorrespondingProxyMethod(x, proxyClass));
                        } else if (a instanceof MutatorAccessor) {
                            methodHashTable.put(((MutatorAccessor) a).name(), findCorrespondingProxyMethod(x, proxyClass));
                        }
                    }
                });
    }


    public P constructSMRObject(ICorfuSMRObject<P> obj)
            throws Exception {
        if (obj == null && !isCorfuObject) {
            if (creationArguments == null) {
                return originalClass.newInstance();
            } else {
                Class[] typeList = Arrays.stream(creationArguments)
                        .map(Object::getClass)
                        .toArray(Class[]::new);

                return originalClass
                        .getDeclaredConstructor(typeList)
                        .newInstance(creationArguments);
            }
        }

        if (stateClass != null) {
            return (P) ReflectionUtils.newInstanceFromUnknownArgumentTypes(stateClass, creationArguments);
        }

        // Is initialObject implemented? If it is, we use that implementation.
        try {
            if (obj == null) {
                throw new UnprocessedException();
            }
            return obj.initialObject(creationArguments);
        } catch (UnprocessedException ue) {
            //it is not, so we search the type hierarchy and call a default constructor.
            Type[] ptA = originalClass.getGenericInterfaces();
            ParameterizedType pt = null;
            for (Type p : ptA) {
                if (((ParameterizedType) p).getRawType().toString()
                        .endsWith("org.corfudb.runtime.object.ICorfuSMRObject")) {
                    pt = (ParameterizedType) p;
                }
            }

            log.trace("Determined SMR type to be {}", pt.getActualTypeArguments()[0].getTypeName());
            if (creationArguments == null) {
                return (P) ((Class) ((ParameterizedType) pt.getActualTypeArguments()[0]).getRawType()
                ).newInstance();
            } else {
                Class[] typeList = Arrays.stream(creationArguments)
                        .map(Object::getClass)
                        .toArray(Class[]::new);

                return (P) ((Class) ((ParameterizedType) pt.getActualTypeArguments()[0]).getRawType())
                        .getDeclaredConstructor(typeList)
                        .newInstance(creationArguments);
            }
        }
    }


    @SuppressWarnings("unchecked")
    @RuntimeType
    public Object interceptGetSMRObject(@This ICorfuSMRObject<P> obj) throws Exception {
        if (smrObject == null && !selfState) {
            smrObject = constructSMRObject(obj);
        }

        if (selfState) {
            return underlyingObject;
        }

        return smrObject;
    }

    @SuppressWarnings("unchecked")
    @RuntimeType
    public void registerPostHandler(ICorfuSMRObject.SMRHandlerMethod method) throws Exception {
        this.postHandler = method;
    }


    @RuntimeType
    public Object interceptMutator(@Origin Method Mmethod,
                                   @AllArguments Object[] allArguments,
                                   @SuperCall Callable superMethod
    ) throws Exception {
        String method = getSMRMethodName(Mmethod);
        log.debug("Object[{}]: +Mutator {} {}", getStreamID(),
                TransactionalContext.isInTransaction() ? "tx" : "", method);
        //StackTraceElement[] stack = new Exception().getStackTrace();
        //if (stack.length > 6 && stack[6].getClassName().equals("org.corfudb.runtime.object.CorfuSMRObjectProxy")) {
         if (methodAccessMode.get()) {
            if (isCorfuObject) {
                return superMethod.call();
            } else {
                return Mmethod.invoke(interceptGetSMRObject(null), allArguments);
            }
        } else if (!TransactionalContext.isInTransaction()) {
            writeUpdate(method, allArguments);
        } else {
            // in a transaction, we add the update to the TX buffer and apply the update
            // immediately.
            if (Mmethod.isAnnotationPresent(Mutator.class)) {
                // if the mutation is a reset, the SMR clone can
                // be a new instance
                Mutator m = Mmethod.getAnnotation(Mutator.class);
                if (m.reset()) {
                }
            }
            doUnderlyingCall(superMethod, Mmethod, allArguments);
        }
        return null;
    }


    @RuntimeType
    public Object interceptMutatorAccessor(
            @Origin Method Mmethod,
            @SuperCall Callable superMethod,
            @AllArguments Object[] allArguments,
            @This P obj) throws Exception {
        String method = getSMRMethodName(Mmethod);
        log.debug("Object[{}] +MutatorAccessor {} {}", getStreamID(),
                TransactionalContext.isInTransaction() ? "tx" : "", method);

        if (methodAccessMode.get()) {
            return doUnderlyingCall(superMethod, Mmethod, allArguments);
        } else if (!TransactionalContext.isInTransaction()) {
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
            return doUnderlyingCall(superMethod, Mmethod, allArguments);
        }
    }


    @RuntimeType
    public Object interceptAccessor(@SuperCall Callable superMethod,
                                    @Origin Method method,
                                    @AllArguments Object[] arguments,
                                    @This P obj) throws Exception {
        log.trace("Object[{}] +Accessor {} {}", getStreamID(), TransactionalContext.isInTransaction() ? "tx" : "", method);
        // Linearize this access with respect to other accesses in the system.
        if (!TransactionalContext.isInTransaction()) {
            sync(obj, Long.MAX_VALUE);
            return doUnderlyingCall(superMethod, method, arguments);
        } else {
            doTransactionalSync(obj);
            Object ret = doUnderlyingCall(superMethod, method, arguments);
            return ret;
        }
    }

    private synchronized Object doUnderlyingCall(Callable superMethod, Method method, Object[] arguments)
            throws Exception {
        if (isCorfuObject || selfState) {
            return superMethod.call();
        } else {
            return method.invoke(interceptGetSMRObject(null), arguments);
        }
    }

    public synchronized void doTransactionalSync(P obj) {
    }

    public String getSMRMethodName(Method method) {
        for (Annotation a : method.getDeclaredAnnotations()) {
            if (a instanceof Mutator) {
                if (!((Mutator) a).name().equals("")) {
                    return ((Mutator) a).name();
                }
            } else if (a instanceof MutatorAccessor) {
                if (!((MutatorAccessor) a).name().equals("")) {
                    return ((MutatorAccessor) a).name();
                }
            }
        }
        return ReflectionUtils.getShortMethodName(method.toString());
    }

    long writeUpdate(String method, Object[] arguments) {
        log.trace("Write update: {} with arguments {}", method, arguments);
        return sv.write(new SMREntry(method, arguments, serializer));
    }

    long writeUpdateAndMapFuture(String method, Object[] arguments) {
        log.trace("Write update and map future: {} with arguments {}", method, arguments);
        return sv.acquireAndWrite(new SMREntry(method, arguments, serializer),
                t -> {
                    completableFutureMap.put(t.getToken(), new CompletableFuture<>());
                    return true;
                },
                t -> {
                    completableFutureMap.remove(t.getToken());
                    return true;
                });
    }

    boolean applySMRUpdate(long address, SMREntry entry, P obj) {
        log.trace("Apply SMR update at {} : {}", address, entry);
        // Look for the uninstrumented method
        try {
            // Find the method by using the method name hash table.
            Method m = methodHashTable.computeIfAbsent(entry.getSMRMethod(),
                    s -> {
                        try {
                            return obj.getClass().getMethod(ReflectionUtils.getMethodNameOnlyFromString(entry.getSMRMethod()),
                                    ReflectionUtils.getArgumentTypesFromString(entry.getSMRMethod()));
                        } catch (NoSuchMethodException nsme) {
                            return null;
                        }
                    });

            if (m == null) {
                throw new NoSuchMethodException(entry.getSMRMethod());
            }
            // Execute the SMR command
            methodAccessMode.set(true);
            Object ret = m.invoke(obj, entry.getSMRArguments());
            methodAccessMode.set(false);
            // Update the current timestamp.
            timestamp = address;
            log.trace("Timestamp for [{}] updated to {}", sv.getStreamID(), address);
            if (completableFutureMap.containsKey(address)) {
                completableFutureMap.get(address).complete(ret);
            }
            if (postHandler != null) {
                postHandler.handle(entry.getSMRMethod(), entry.getSMRArguments(), obj);
            }
            return true;
        } catch (NoSuchMethodException n) {
            log.error("Couldn't find method {} during apply update", entry.getSMRMethod(), n);
            if (completableFutureMap.containsKey(address)) {
                completableFutureMap.get(address).completeExceptionally(n);
            }
        } catch (InvocationTargetException | IllegalAccessException iae) {
            log.error("Couldn't dispatch method {} during apply update", entry.getSMRMethod(), iae);
            if (completableFutureMap.containsKey(address)) {
                completableFutureMap.get(address).completeExceptionally(iae);
            }
        } catch (Exception e) {
            log.warn("Exception during application of SMR method {}", entry.getSMRMethod());
            if (completableFutureMap.containsKey(address)) {
                completableFutureMap.get(address).completeExceptionally(e);
            }
        }
        return false;
    }

    boolean applyUpdate(long address, LogEntry entry, P obj) {
        if (entry instanceof ISMRConsumable)
        {
            ((ISMRConsumable) entry).getSMRUpdates(getStreamID())
                    .stream()
                    .forEach(x -> applySMRUpdate(address, x, obj));
        }
        else {
            log.warn("Non SMR entry of type={} encountered", entry.getClass());
        }
        return true;
    }

    @Override
    public synchronized void sync(P obj, long maxPos) {
        try (LockUtils.AutoCloseRWLock writeLock = new LockUtils.AutoCloseRWLock(rwLock).writeLock()) {
            ILogData[] entries = sv.readTo(maxPos);
            log.trace("Object[{}] sync to pos {}, read {} entries",
                    sv.getStreamID(), maxPos == Long.MAX_VALUE ? "MAX" : maxPos, entries.length);
            Arrays.stream(entries)
                    .filter(m -> m.getType() == DataType.DATA)
                    .filter(m -> m.getPayload(runtime) instanceof ISMRConsumable)
                    .forEach(m -> applyUpdate(m.getGlobalAddress(), (LogEntry) m.getPayload(runtime), obj));
        }

    }
}

package org.corfudb.runtime.object;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.logprotocol.TXEntry;
import org.corfudb.protocols.logprotocol.TXLambdaReferenceEntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogUnitEntry;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.UnprocessedException;
import org.corfudb.runtime.object.transactions.LambdaTransactionalContext;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.StreamView;
import org.corfudb.util.LockUtils;
import org.corfudb.util.ReflectionUtils;
import org.corfudb.util.serializer.Serializers;

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
                               Class<P> originalClass, Serializers.SerializerType serializer) {
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

    public Object findTransactionalSMRObject() {
        for (StackTraceElement ste : new Exception().getStackTrace()) {
            if (ste.getMethodName().equals("interceptAccessor")) {
                return TransactionalContext.getCurrentContext().getObjectRead(this);
            } else if (ste.getMethodName().equals("interceptMutator")) {
                return TransactionalContext.getCurrentContext().getObjectWrite(this);
            } else if (ste.getMethodName().equals("interceptMutatorAccessor")) {
                return TransactionalContext.getCurrentContext().getObjectReadWrite(this);
            }
        }
        return TransactionalContext.getCurrentContext().getObjectReadWrite(this);
    }

    @SuppressWarnings("unchecked")
    @RuntimeType
    public Object interceptGetSMRObject(@This ICorfuSMRObject<P> obj) throws Exception {
        if (smrObject == null && !selfState) {
            smrObject = constructSMRObject(obj);
        }

        if (TransactionalContext.isInTransaction()
                && !TransactionalContext.getCurrentContext().isInSyncMode()) {
            return findTransactionalSMRObject();
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
        StackTraceElement[] stack = new Exception().getStackTrace();
        if (stack.length > 6 && stack[6].getClassName().equals("org.corfudb.runtime.object.CorfuSMRObjectProxy")) {
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
                    TransactionalContext.getCurrentContext()
                            .resetObject(this);
                }
            }
            TransactionalContext.getCurrentContext().bufferObjectUpdate(this,
                    method, allArguments, serializer, true);
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

        StackTraceElement[] stack = new Exception().getStackTrace();
        if (stack.length > 6 && stack[6].getClassName().equals("org.corfudb.runtime.object.CorfuSMRObjectProxy")) {
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
            TransactionalContext.getCurrentContext().bufferObjectUpdate(CorfuSMRObjectProxy.this,
                    method, allArguments, serializer, false);
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
            // If the object was written to (due to transactional clone), the read set is not resolvable.
            if (!TransactionalContext.getCurrentContext().isObjectCloned(this)) {
                // Store the read set with the context.
                TransactionalContext.getCurrentContext().addReadSet(this,
                        getSMRMethodName(method), ret);
            }
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
        if (TransactionalContext.isInOptimisticTransaction()) {
            TransactionalContext.getCurrentContext().setInSyncMode(true);
            // Otherwise we should make sure we're sync'd up to the TX
            sync(obj, TransactionalContext.getCurrentContext().getFirstReadTimestamp());
            TransactionalContext.getCurrentContext().setInSyncMode(false);
        }
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
            Object ret = m.invoke(obj, entry.getSMRArguments());
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
        if (entry instanceof SMREntry) {
            return applySMRUpdate(address, (SMREntry) entry, obj);
        } else if (entry instanceof TXEntry) {
            TXEntry txEntry = (TXEntry) entry;
            log.trace("Apply TX update at {}: {}", address, txEntry);
            // First, determine if the TX is abort.
            if (txEntry.isAborted()) {
                return false;
            }
            txEntry.getTxMap().get(sv.getStreamID())
                    .getUpdates().stream()
                    .forEach(x -> applySMRUpdate(address, x, obj));

            return true;
        } else if (entry instanceof TXLambdaReferenceEntry) {
            log.debug("Apply TXLambdaRef {} at {}", ((TXLambdaReferenceEntry) entry).getMethod().toString(), address);
            try (TXLambdaReferenceEntry.LambdaLock ll = TXLambdaReferenceEntry.getLockForTXAddress(address)) {
                // unlock the sync lock :::
                // TODO: fixme this is ugly
                rwLock.writeLock().unlock();
                try {
                    ll.getLock().lock();
                    // check if the timestamp has moved past this lambda ref (due to another thread applying the same TX)
                    log.info("Object[{}]: execute TXLambdaRef@{}", getStreamID(), address);
                    if (timestamp < address) {
                        TransactionalContext.newContext(new LambdaTransactionalContext(runtime, address));
                        ((TXLambdaReferenceEntry) entry).invoke();
                        TransactionalContext.removeContext();
                    }
                } finally {
                    rwLock.writeLock().lock();
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public void sync(P obj, long maxPos) {
        try (LockUtils.AutoCloseRWLock writeLock = new LockUtils.AutoCloseRWLock(rwLock).writeLock()) {
            LogData[] entries = sv.readTo(maxPos);
            log.trace("Object[{}] sync to pos {}, read {} entries",
                    sv.getStreamID(), maxPos == Long.MAX_VALUE ? "MAX" : maxPos, entries.length);
            Arrays.stream(entries)
                    .filter(m -> m.getType() == DataType.DATA)
                    .filter(m -> m.getPayload(runtime) instanceof SMREntry ||
                            m.getPayload(runtime) instanceof TXEntry || m.getPayload(runtime) instanceof TXLambdaReferenceEntry)
                    .forEach(m -> applyUpdate(m.getGlobalAddress(), (LogEntry) m.getPayload(runtime), obj));
        }
    }
}

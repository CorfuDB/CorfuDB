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
import org.corfudb.util.ReflectionUtils;
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
public class CorfuSMRObjectProxy<P> extends CorfuObjectProxy<P> {

    Object smrObject;

    Object[] creationArguments;

    Map<Long, CompletableFuture<Object>> completableFutureMap;

    @Getter
    Map<String, Method> methodHashTable;

    @Getter
    boolean isCorfuObject = false;

    public CorfuSMRObjectProxy(CorfuRuntime runtime, StreamView sv,
                               Class<P> originalClass, Serializers.SerializerType serializer) {
        super(runtime, sv, originalClass, serializer);
        this.completableFutureMap = new ConcurrentHashMap<>();
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
                return TransactionalContext.getCurrentContext().getObjectRead(this);
            }
            else if (name.equals("interceptMutator")){
                return TransactionalContext.getCurrentContext().getObjectWrite(this);
            }
            else {
                return TransactionalContext.getCurrentContext().getObjectReadWrite(this);
            }
        }
        return smrObject;
    }

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
                return Mmethod.invoke(interceptGetSMRObject(null), allArguments);
            }
        }
        else if (!TransactionalContext.isInTransaction()){
            writeUpdate(method, allArguments);
        }
        else {
            // in a transaction, we add the update to the TX buffer and apply the update
            // immediately.
            TransactionalContext.getCurrentContext().bufferObjectUpdate(this,
                    method, allArguments, serializer);
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
            if (!TransactionalContext.getCurrentContext().isObjectCloned(this))
            {
                // Store the read set with the context.
                TransactionalContext.getCurrentContext().addReadSet(this,
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
            return method.invoke(interceptGetSMRObject(null), arguments);
        }
    }

    public synchronized void doTransactionalSync(P obj) {
        TransactionalContext.getCurrentContext().setInSyncMode(true);
        // Otherwise we should make sure we're sync'd up to the TX
        sync(obj, TransactionalContext.getCurrentContext().getFirstReadTimestamp());
        TransactionalContext.getCurrentContext().setInSyncMode(false);
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
        return ReflectionUtils.getShortMethodName(method.toString());
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
                        return obj.getClass().getMethod(ReflectionUtils.getMethodNameOnlyFromString(entry.getSMRMethod()),
                                ReflectionUtils.getArgumentTypesFromString(entry.getSMRMethod()));
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

    @Override
    synchronized public void sync(P obj, long maxPos) {
        log.trace("Object[{}] sync to pos {}", sv.getStreamID(), maxPos == Long.MAX_VALUE ? "MAX" : maxPos);
        Arrays.stream(sv.readTo(maxPos))
                .filter(m -> m.getResultType() == LogUnitReadResponseMsg.ReadResultType.DATA)
                .filter(m -> m.getPayload() instanceof SMREntry ||
                        m.getPayload() instanceof TXEntry)
                .forEach(m -> applyUpdate(m.getAddress(), (LogEntry) m.getPayload(), obj));
    }
}

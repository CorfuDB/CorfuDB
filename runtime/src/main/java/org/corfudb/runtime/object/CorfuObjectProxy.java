package org.corfudb.runtime.object;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;
import org.corfudb.annotations.TransactionalMethod;
import org.corfudb.protocols.logprotocol.TXLambdaReferenceEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * Created by mwei on 3/30/16.
 */
@Slf4j
@Deprecated
public class CorfuObjectProxy<P> {

    /**
     * The underlying streamView for this CorfuObject.
     */
    @Getter
    IStreamView sv;

    /**
     * The original class which this class proxies.
     */
    @Getter
    Class<P> originalClass;

    /**
     * The serializer used by this proxy when it is written to the log.
     */
    @Getter
    ISerializer serializer;

    /**
     * The runtime used to create this proxy.
     */
    @Getter
    CorfuRuntime runtime;

    /**
     * The current timestamp of this proxy.
     */
    @Getter
    @Setter
    long timestamp;

    /** The access mode of this proxy, per thread.
     *
     */
    @Getter
    ThreadLocal<Boolean> methodAccessMode = ThreadLocal.withInitial(() -> false);

    /**
     * The generated proxy class that this proxy wraps around.
     */
    @Getter
    @Setter
    Class<? extends P> generatedClass;

    public CorfuObjectProxy(CorfuRuntime runtime, IStreamView sv,
                            Class<P> originalClass, ISerializer serializer) {
        this.runtime = runtime;
        this.sv = sv;
        this.originalClass = originalClass;
        this.serializer = serializer;
        this.timestamp = -1L;
    }

    @RuntimeType
    @SuppressWarnings("unchecked")
    public Object handleTransactionalMethod(@SuperCall Callable originalCall,
                                            @Origin Method method,
                                            @AllArguments Object[] arguments,
                                            @This ICorfuObject obj)
            throws Exception {
        //if (TransactionalContext.isInOptimisticTransaction()) {
            // TODO: in an optimistic TX, insert a Lambda TXn entry instead of converting everything
            // into a writeset
         //   log.debug("Optimistic TXn, flatten TX into append set.");
        //    return originalCall.call();
        //}
        // TODO: can we get rid of this origin based call?
        boolean invoked = Arrays.stream(new Exception().getStackTrace())
                .map(StackTraceElement::getClassName)
                .anyMatch(e -> e.endsWith("TXLambdaReferenceEntry"));

        // If we were called from TXLambdaReferenceEntry::invoke...
        if (invoked) {
            log.debug("Redirect to original TX call");
            return originalCall.call();
        }
        Object res;
        TransactionalMethod tm = method.getAnnotation(TransactionalMethod.class);
        if (false) {
            // 1) Find the annotated streams function
            // case A: takes no arguments.
            Method m = originalClass.getDeclaredMethod(tm.modifiedStreamsFunction());
            m.setAccessible(true);
            Set<UUID> affectedStreams = (Set<UUID>) m.invoke(obj);
            // append the transaction to the log
            log.trace("TX Method: {}, Affected streams: {}", method.getName(), affectedStreams);
            TXLambdaReferenceEntry tlre = new TXLambdaReferenceEntry(method, obj,
                    arguments, Serializers.JSON);
            // if the TX returns something, we need to join on a completable future.
            if (!method.getReturnType().getName().equals("void")) {
                CompletableFuture cf = new CompletableFuture();
                long txAddr = runtime.getStreamsView().acquireAndWrite(affectedStreams, tlre, t -> {
                    runtime.getObjectsView().getTxFuturesMap().put(t.getToken(), cf);
                    return true;
                }, t -> {
                    runtime.getObjectsView().getTxFuturesMap().remove(t.getToken());
                    return true;
                });
                log.debug("Wrote TX to log@{}", txAddr);
                // pick the first affected object and sync.
                ICorfuObject cobj = (ICorfuObject) runtime.getObjectsView().getObjectCache().entrySet().stream()
                        .filter(x -> affectedStreams.contains(x.getKey().getStreamID()))
                        .findFirst().get().getValue();
                cobj.getProxy().sync(cobj, txAddr);
                return cf.join();
            } else {
                // TX doesn't return anything, so we can blindly append to the log.
                long txAddr = runtime.getStreamsView().write(affectedStreams, tlre);
                log.debug("Wrote TX to log@{}", txAddr);
                return null;
            }

        } else {
            try {
                runtime.getObjectsView().TXBegin();
                res = originalCall.call();
                runtime.getObjectsView().TXEnd();
            } catch (TransactionAbortedException tae) {
                log.debug("Transactional method aborted due to {}, retrying.", tae);
                res = handleTransactionalMethod(originalCall, method, arguments, obj);
            }
            return res;
        }
    }

    synchronized public void sync(P obj, long maxPos) {
        log.trace("CorfuObjectProxy[{}] sync to pos {}", sv.getID(), maxPos == Long.MAX_VALUE ? "MAX" : maxPos);
        sv.remainingUpTo(maxPos);
    }

    public UUID getStreamID() {
        return sv.getID();
    }

    public CorfuObjectProxy getProxy() {
        return this;
    }
}

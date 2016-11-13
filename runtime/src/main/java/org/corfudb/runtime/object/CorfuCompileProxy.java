package org.corfudb.runtime.object;

import com.squareup.javapoet.ParameterizedTypeName;
import io.netty.util.internal.ConcurrentSet;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.ISMRConsumable;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.StreamView;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;

/**
 * Created by mwei on 11/11/16.
 */
@Slf4j
public class CorfuCompileProxy<T> implements ICorfuSMRProxy<T> {

    Object lastTransactionalUpcallResult = null;
    ThreadLocal<Boolean> threadInTransaction = ThreadLocal.withInitial(() -> false);

    VersionLockedObject<T> underlyingObject;

    CorfuRuntime rt;
    UUID streamID;
    Class<T> type;
    ISerializer serializer;
    Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap;
    Object[] args;
    Set<Long> pendingUpcalls;

    // This enum is necessary because null cannot be inserted
    // into a ConcurrentHashMap.
    enum NullValue {
        NULL_VALUE
    }

    Map<Long, Object> upcallResults;
    StampedLock txLock;

    public CorfuCompileProxy(CorfuRuntime rt, UUID streamID, Class<T> type, Object[] args,
                             ISerializer serializer,
                             Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap) {
        this.rt = rt;
        this.streamID = streamID;
        this.type = type;
        this.args = args;
        this.serializer = serializer;
        this.upcallTargetMap = upcallTargetMap;
        this.pendingUpcalls = new ConcurrentSet<>();
        this.upcallResults = new ConcurrentHashMap<>();

        this.txLock = new StampedLock();

        underlyingObject = getNewObject();
    }

    @SuppressWarnings("unchecked")
    private VersionLockedObject<T> getNewObject() {
        try {
            T ret = null;
            if (args == null || args.length == 0) {
                ret = type.newInstance();
            } else {
                // This loop is not ideal, but the easiest way to get around Java boxing,
                // which results in primitive constructors not matching.
                for (Constructor<?> constructor : type.getDeclaredConstructors()) {
                    try {
                        ret = (T) constructor.newInstance(args);
                        break;
                    } catch (Exception e) {
                       // just keep trying until one works.
                    }
                }
            }
            if (ret instanceof ICorfuSMRProxyContainer) {
                ((ICorfuSMRProxyContainer<T>) ret).setProxy$CORFUSMR(this);
            }
            return new VersionLockedObject<T>(ret, -1L, rt.getStreamsView().get(streamID));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void rollback(AbstractTransactionalContext txContext) {
        // Well, it's always safe to just create the object from scratch.
        try {
            VersionLockedObject<T> obj = getNewObject();
            long latestToken = rt.getSequencerView()
                    .nextToken(Collections.singleton(streamID), 0).getToken();
            rawAccess(obj, latestToken, e -> null, true, false);
            underlyingObject = obj;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Access the state of the object. If accessMethod is null, it is expected that
     * the result of the upcall at timestamp is desired.
     *
     * @param timestamp    The timestamp to access the object at.
     * @param accessMethod The method to execute when accessing an object.
     * @return
     */
    @Override
    public <R> R access(long timestamp, ICorfuSMRAccess<R, T> accessMethod) {
        // If we're in a optimistic transactional context, we should record this access
        // if it's a read, and redirect the access to the transactional object.
        if (TransactionalContext.isInOptimisticTransaction()) {
                if (!threadInTransaction.get()) {
                    // Lock this object for TX modification
                    // Note that this will prevent other threads from
                    // accessing this object during our TX, which COULD (check this, probably not.)
                    // lead to deadlock if there is a dependency.
                    // The only way to avoid this is to create a clone during the
                    // TX, which is expensive. We can also wait awhile and create
                    // the object from scratch (or a snapshot) if there really is a
                    // conflict...
                    long ls = txLock.writeLock();
                    // Only two things will cause us to unlock this object:
                    // Either the transaction commits or aborts.

                    // When the transaction commits, we can unlock (only for unnested)
                    TransactionalContext.getCurrentContext()
                            .addPostCommitAction((x, ts) -> {
                                // If not a read-only transaction, update the version.
                                if (ts != -1L) {
                                    underlyingObject.setVersionUnsafe(ts);
                                }
                                if (!TransactionalContext.isInNestedTransaction()) {
                                    threadInTransaction.set(false);
                                }
                                txLock.unlock(ls);
                            });


                    // When the transaction aborts, we must roll back and then unlock.
                    TransactionalContext.getCurrentContext()
                            .addPostAbortAction(x -> {
                                rollback(x);
                                if (!TransactionalContext.isInNestedTransaction()) {
                                    threadInTransaction.set(false);
                                }
                                txLock.unlock(ls);
                            });

                    // Make sure that our version is equal to the
                    // firstReadTimestamp, to guarantee snapshot isolation.
                    long firstRead = TransactionalContext.getCurrentContext()
                            .getFirstReadTimestamp();

                    if (firstRead != underlyingObject.getVersionUnsafe()) {
                        // Okay, several things can happen here as we attempt
                        // to sync to the firstReadTimestamp

                        // The easy case is that we're behind. So all we need
                        // to do is sync forward.
                        if (underlyingObject.getVersionUnsafe()
                                < TransactionalContext.getCurrentContext()
                                .getFirstReadTimestamp()) {
                            rawAccess(underlyingObject, timestamp, e -> null, true, false);
                            // Explicitly set our version number to the firstRead
                            // So we don't sync again.
                            underlyingObject.setVersionUnsafe(firstRead);
                        }
                        // The hard case is if we're ahead. For now, we'll just
                        // dump our state and then sync forward.
                        else {
                            underlyingObject = getNewObject();
                            rawAccess(underlyingObject, firstRead, e -> null, true, false);
                            underlyingObject.setVersionUnsafe(firstRead);
                        }
                    }
                    threadInTransaction.set(true);
                }

            TransactionalContext.getCurrentContext()
                    .markProxyRead(this);

                // Okay, so now we need to check if we have pending updates
                // to this object we need to apply. If so, we apply them...
                Arrays.stream(TransactionalContext.getCurrentContext()
                        .readTransactionLog(getStreamID(), TransactionalContext.getCurrentContext()
                                .getProxyPointer(this)))
                        .forEachOrdered(x -> {
                            try {
                                ICorfuSMRUpcallTarget<T> target = upcallTargetMap.get(x.getSMRMethod());
                                if (target == null) {throw new Exception("Unknown upcall " + x.getSMRMethod());}
                                lastTransactionalUpcallResult = target
                                        .upcall(underlyingObject.getObjectUnsafe(),
                                        x.getSMRArguments());
                            }
                            catch (Exception e) {
                                log.error("Error: Couldn't execute upcall due to {}", e);
                                throw new RuntimeException(e);
                            }
                        });

            TransactionalContext.getCurrentContext()
                    .setProxyPointer(this, TransactionalContext
                            .getCurrentContext().getTransactionLogSize(getStreamID()));

                if (accessMethod == null) {
                    // Because transactions force a single thread,
                    // Recording just the last upcall is sufficient.
                    return (R) lastTransactionalUpcallResult;
                }
                else {
                    return accessMethod.access(underlyingObject.getObjectUnsafe());
                }
            }


        // Might be a good idea to figure out what a good timeout value is.
        try {
            long ls = txLock.tryReadLock(1, TimeUnit.SECONDS);
            if (ls == 0) {throw new InterruptedException();} //lock acquisition failed
            try {
                return rawAccess(underlyingObject, timestamp, accessMethod, false, false);
            } finally {
                txLock.unlockRead(ls);
            }
        } catch (InterruptedException e) {
            // do raw access.
            try {
                return rawAccess(getNewObject(), timestamp, accessMethod, false, false);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }


    /** Update the object. Ensure that you have the write lock before calling
     * this function...
     * @param underlyingObject  The object to update.
     * @param timestamp         The timestamp to update the object to.
     * @param noUpdate          True, if object timestamp should not be updated (transactions).
     */
    private <R> void updateObject(VersionLockedObject<T> underlyingObject,
                              long timestamp, boolean noUpdate) {
        Arrays.stream(underlyingObject.getStreamViewUnsafe().readTo(timestamp))
            // Turn this into a flat stream of SMR entries
            .filter(m -> m.getType() == DataType.DATA)
            .filter(m -> m.getPayload(rt) instanceof ISMRConsumable)
            .forEach(logData -> {
                ((ISMRConsumable)logData.getPayload(rt)).getSMRUpdates(getStreamID()).stream()
                    .map(c -> c.getSMRUpdates(streamID))
                    .flatMap(List::stream)
                    // Apply each entry, in order into the underlyingObject.
                    .forEachOrdered(l -> {
                        try {
                            ICorfuSMRUpcallTarget<T> target = upcallTargetMap.get(l.getSMRMethod());
                            if (target == null) {
                                throw new Exception("Unknown upcall " + l.getSMRMethod());
                            }
                            R res = (R) target.upcall(underlyingObject
                                    .getObjectUnsafe(), l.getSMRArguments());
                            if (!noUpdate) {
                                Long streamAddress = logData.getStreamAddress(streamID);
                                if (streamAddress != null) {
                                    underlyingObject.setVersionUnsafe(streamAddress);
                                } else {
                                    underlyingObject.setVersionUnsafe(logData.getGlobalAddress());
                                }
                                underlyingObject.setGlobalVersionUnsafe(logData.getGlobalAddress());
                            }

                            upcallResults.put(underlyingObject.getGlobalVersionUnsafe(), res == null ?
                                        NullValue.NULL_VALUE : res);
                        } catch (Exception e) {
                            log.error("Error: Couldn't execute upcall due to {}", e);
                            throw new RuntimeException(e);
                        }
                    });
            });
    }

    /**
     * Access the raw state of the object, ignoring any transactions.
     * If accessMethod is null, it is expected that
     * the result of the upcall at timestamp is desired.
     *
     * @param underlyingObject  The object to execute against.
     * @param requestTimestamp         The timestamp to access the object at.
     * @param accessMethod      The method to execute when accessing an object.
     * @param noLock            True, if locking is not required.
     * @param noUpdate          True, if the timestamp should not be updated.
     * @return
     */
    private <R> R rawAccess(VersionLockedObject<T> underlyingObject, long requestTimestamp,
                            ICorfuSMRAccess<R, T> accessMethod, boolean noLock, boolean noUpdate) {
        // If we want the most recent
        // version, first figure out what the most recent version is...
        if (requestTimestamp == Long.MAX_VALUE) {
            TokenResponse tr =
                    rt.getSequencerView().nextToken(Collections.singleton(streamID), 0);
            requestTimestamp = tr.getToken();
        }

        final long timestamp = requestTimestamp;

        // If we just want an upcall result, check if we already have it.
        // This is always safe, since we're good if we have
        // the result.
        if (accessMethod == null && upcallResults.containsKey(timestamp)) {
            R ret = (R) upcallResults.get(timestamp);
            upcallResults.remove(timestamp);
            return ret == NullValue.NULL_VALUE ? null : ret;
        }

        // If noLock was requested (presumably because we already have
        // a lock, like in a transaction)
        if (noLock) {
            // If we're at the correct version, just return it
            if (underlyingObject.getVersionUnsafe() == timestamp
                    && accessMethod != null) {
                return accessMethod.access(underlyingObject.getObjectUnsafe());
            }
            // Or force an update
            updateObject(underlyingObject, timestamp, noUpdate);
            if (accessMethod == null) {
                if (upcallResults.containsKey(timestamp)) {
                    R ret = (R) upcallResults.get(timestamp);
                    upcallResults.remove(timestamp);
                    return ret == NullValue.NULL_VALUE ? null : ret;
                } else {
                    // The version is already ahead, but we don't have the result.
                    // The only way to get the correct result
                    // of the upcall would be to rollback. For now, we throw an exception
                    // since this is generally not expected. --- and probably a bug if it happens.
                    throw new RuntimeException("Attempted to get the result " +
                            "of an upcall@" + timestamp +" which" +
                            "was already executed and we don't have a copy");
                }
            }
            else {
                accessMethod.access(underlyingObject.getObjectUnsafe());
            }
        }

        // If we're certain we've already gone past the version we need,
        // we just need to wait for the writer (updating thread) to apply
        // the upcall so we have the result. The writer should have the wlock, so rlock
        if (accessMethod == null && underlyingObject.getVersionUnsafe() > timestamp)
        {
            underlyingObject.waitOnLock();
            // after the wlock is released, the upcall result _should_ be in the table.
            if (upcallResults.containsKey(timestamp)) {
                R ret = (R) upcallResults.get(timestamp);
                upcallResults.remove(timestamp);
                return ret == NullValue.NULL_VALUE ? null : ret;
            } else {
                    // The version is already ahead, but we don't have the result.
                    // The only way to get the correct result
                    // of the upcall would be to rollback. For now, we throw an exception
                    // since this is generally not expected. --- and probably a bug if it happens.
                    throw new RuntimeException("Attempted to get the result " +
                            "of an upcall@" + timestamp +" which" +
                            "was already executed and we don't have a copy");
            }
        }

        // If we're looking for an upcall and we don't have it yet, it probably
        // means we need to update the object, so just lock it for write and update
        // to the latest version.
        if (accessMethod == null) {
            underlyingObject.write((ver, obj) -> {
                updateObject(underlyingObject, timestamp, noUpdate);
                return null;
            });

            // the upcall map should contain the upcall now.
            if (upcallResults.containsKey(timestamp)) {
                R ret = (R) upcallResults.get(timestamp);
                upcallResults.remove(timestamp);
                return ret == NullValue.NULL_VALUE ? null : ret;
            }
            throw new RuntimeException("Attempted to get the result " +
                    "of an upcall@" + timestamp +" but we couldn't obtain it (obj@" +
                    underlyingObject.getVersionUnsafe() + ")!");
        } else {
            // otherwise we're an accessor and maybe we're already at the right
            // version.
            return underlyingObject.optimisticallyReadAndWriteOnFail(
                    // First, see if an optimistic read picks up the
                    // correct version.
                    (ver, obj) -> {
                        if (ver == timestamp) {
                            return accessMethod.access(obj);
                        } else {
                            throw new ConcurrentModificationException();
                        }
                    },
                    // Okay, so we have the wrong version, and we need
                    // to update. We have a write lock now.
                    (ver, obj) -> {
                        // Maybe if we we're lucky, someone _just_ changed
                        // the object to the correct version.
                        if (ver == timestamp) {
                            return accessMethod.access(obj);
                        } else {
                            // So of course, we're probably not so lucky
                            // and we need to update the object before
                            // we can access it.
                            updateObject(underlyingObject, timestamp, noUpdate);
                            return accessMethod.access(obj);
                        }
                    }
            );
        }
    }

    /**
     * Record an SMR function to the log before returning.
     *
     * @param smrUpdateFunction The name of the function to record.
     * @param args              The arguments to the function.
     * @return The address in the log the SMR function was recorded at.
     */
    @Override
    public long logUpdate(String smrUpdateFunction, Object... args) {
        // We're in an optimistic transaction, so we buffer the update instead.
        if (TransactionalContext.isInOptimisticTransaction())
        {
            return TransactionalContext.getCurrentContext()
                    .bufferUpdate(streamID, smrUpdateFunction, args, serializer);
        }

        // If we aren't in a transaction, we can just write the modification.
        // We need to add the acquired token into the pending upcall list.
        SMREntry smrEntry = new SMREntry(smrUpdateFunction, args, serializer);
        long address = underlyingObject.getStreamViewUnsafe().acquireAndWrite(smrEntry, t -> {
                pendingUpcalls.add(t.getToken());
                return true;
            }, t -> {
                pendingUpcalls.remove(t.getToken());
                return true;
            });
        log.trace("Update {} written to {}", smrUpdateFunction, address);
        return address;
    }

    /**
     * Get the ID of the stream this proxy is subscribed to.
     *
     * @return The UUID of the stream this proxy is subscribed to.
     */
    @Override
    public UUID getStreamID() {
        return streamID;
    }

    /**
     * Run in a transactional context.
     *
     * @param txFunction The function to run in a transactional context.
     * @return The value supplied by the function.
     */
    @Override
    public <R> R TXExecute(Supplier<R> txFunction) {
        while (true) {
            try {
                rt.getObjectsView().TXBegin();
                R ret = txFunction.get();
                rt.getObjectsView().TXEnd();
                return ret;
            } catch (Exception e) {
                log.warn("Transactional function aborted due to {}, retrying", e);
                try {Thread.sleep(1000); }
                catch (Exception ex) {}
            }
        }
    }

    /**
     * Get an object builder to build new objects.
     *
     * @return An object which permits the construction of new objects.
     */
    @Override
    public IObjectBuilder<?> getObjectBuilder() {
        return rt.getObjectsView().build();
    }

    /**
     * Return the type of the object being replicated.
     *
     * @return The type of the replicated object.
     */
    @Override
    public Class<T> getObjectType() {
        return type;
    }

    /**
     * Get the latest version read by the proxy.
     *
     * @return The latest version read by the proxy.
     */
    @Override
    public long getVersion() {
        return underlyingObject.getVersionUnsafe();
    }
}

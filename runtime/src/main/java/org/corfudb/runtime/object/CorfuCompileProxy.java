package org.corfudb.runtime.object;

import io.netty.util.internal.ConcurrentSet;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.ISMRConsumable;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.NoRollbackException;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.util.serializer.ISerializer;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static java.lang.Long.min;

/**
 * Created by mwei on 11/11/16.
 */
@Slf4j
public class CorfuCompileProxy<T> implements ICorfuSMRProxyInternal<T> {

    /** The underlying object. This object stores the actual
        state as well as the version of the object. It also
        provides locks to access the object safely from a
        multi-threaded context. */
    @Getter
    VersionLockedObject<T> underlyingObject;

    /** The CorfuRuntime. This allows us to interact with the
     * Corfu log.
     */
    CorfuRuntime rt;

    /** The ID of the stream of the log.
     */
    UUID streamID;

    /** The type of the underlying object. We use this to instantiate
     * new instances of the underlying object.
     */
    Class<T> type;

    /** The serializer SMR entries will use to serialize their
     * arguments.
     */
    ISerializer serializer;

    /** A map containing upcall targets. This takes a SMRMethod string
     * and converts it to an upcall function which transitions an object
     * to a new state given arguments.
     */
    @Getter
    final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap;

    /** An undo target map. This map contains functions which take a
     * SMRMethod string and arguments and reverses a transition given
     * an undoRecord.
     */
    @Getter
    final Map<String, IUndoFunction<T>> undoTargetMap;

    /** An undo record target map. This map contains functions which
     * take a SMRMethod string and arguments and generates an undoRecord
     * which is used by the undo functions.
     */
    @Getter
    final Map<String, IUndoRecordFunction<T>> undoRecordTargetMap;

    /** The arguments this proxy was created with.
     *
     */
    final Object[] args;

    /** A list of upcalls pending in the system. The proxy keeps this
     * set so it can remember to save the upcalls for pending requests.
     */
    Set<Long> pendingUpcalls;

    // This enum is necessary because null cannot be inserted
    // into a ConcurrentHashMap.
    enum NullValue {
        NULL_VALUE
    }

    /** A list of upcall results, keyed by the address they were
     * requested.
     */
    Map<Long, Object> upcallResults;

    public CorfuCompileProxy(CorfuRuntime rt, UUID streamID, Class<T> type, Object[] args,
                             ISerializer serializer,
                             Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap,
                             Map<String, IUndoFunction<T>> undoTargetMap,
                             Map<String, IUndoRecordFunction<T>> undoRecordTargetMap
                             ) {
        this.rt = rt;
        this.streamID = streamID;
        this.type = type;
        this.args = args;
        this.serializer = serializer;

        this.upcallTargetMap = upcallTargetMap;
        this.undoTargetMap = undoTargetMap;
        this.undoRecordTargetMap = undoRecordTargetMap;

        this.pendingUpcalls = new ConcurrentSet<>();
        this.upcallResults = new ConcurrentHashMap<>();

        underlyingObject = getNewVersionLockedObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R> R access(ICorfuSMRAccess<R, T> accessMethod,
                        Object[] conflictObject) {
        if (TransactionalContext.isInTransaction()) {
            return TransactionalContext.getCurrentContext()
                    .access(this, accessMethod, conflictObject);
        }

        // Linearize this read against a timestamp
        final long timestamp =
                rt.getSequencerView()
                .nextToken(Collections.singleton(streamID), 0).getToken();
        log.debug("access [{}] at ts {}", getStreamID(), timestamp);

        // Acquire locks and perform read.
        return underlyingObject.optimisticallyReadThenReadLockThenWriteOnFail(
             (ver,o) -> {
            // If not in a transaction, check if the version is
            // at least that of the linearized read requested.
            if (ver >= timestamp
                    && !underlyingObject.isOptimisticallyModifiedUnsafe()) {
                return accessMethod.access(underlyingObject.getObjectUnsafe());
            }
            // We don't have the right version, so we need to write
            // throwing this exception causes us to take a write lock.
                 log.debug("access needs to sync forward up to timestamp {}", timestamp);
            throw new ConcurrentModificationException();
        },
            //  The read did not acquire the right version, so we
            //  have now acquired a write lock.
        (ver, o) -> {
            // In the off chance that someone updated the version
            // for us.
            if (ver >= timestamp
                    && !underlyingObject.isOptimisticallyModifiedUnsafe()) {
                return accessMethod.access(underlyingObject.getObjectUnsafe());
            }

            // Now we sync forward while we have the lock, if the object
            // was not optimistically modified
            if (!underlyingObject.isOptimisticallyModifiedUnsafe()) {
                syncObjectUnsafe(underlyingObject, timestamp);
                return accessMethod.access(underlyingObject.getObjectUnsafe());

            }
            // Otherwise, we rollback any optimistic changes, if they
            // are undoable, and then sync the object before accessing
            // the object.
            else if (underlyingObject.isOptimisticallyUndoableUnsafe()){
                try {
                    underlyingObject.optimisticRollbackUnsafe();
                    syncObjectUnsafe(underlyingObject, timestamp);
                    // do the access
                    R ret = accessMethod
                            .access(underlyingObject.getObjectUnsafe());
                    return ret;
                } catch (NoRollbackException nre) {
                    // We couldn't roll back, so we'll have to
                    // resort to generating a new object.
                }
            }

            // As a last resort, we'll have to generate a new object
            // and replay. The object will be disposed.
            VersionLockedObject<T> temp = getNewVersionLockedObject();
            syncObjectUnsafe(temp, timestamp);
            return accessMethod.access(temp.getObjectUnsafe());
        });
    }


    /** Update the object. Ensure that you have the write lock before calling
     * this function...
     * @param underlyingObject  The object to update.
     * @param timestamp         The timestamp to update the object to.
     */
    public void syncObjectUnsafe(VersionLockedObject<T> underlyingObject,
                                      long timestamp) {
        underlyingObject.getStreamViewUnsafe().remainingUpTo(timestamp).stream()
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
                            Object res = underlyingObject.applyUpdateUnsafe(l, false);
                            underlyingObject.setVersionUnsafe(logData.getGlobalAddress());

                            if (pendingUpcalls.contains(logData.getGlobalAddress())) {
                                upcallResults.put(underlyingObject.getVersionUnsafe(), res == null ?
                                        NullValue.NULL_VALUE : res);
                                pendingUpcalls.remove(logData.getGlobalAddress());
                            }
                        } catch (Exception e) {
                            log.error("Error: Couldn't execute upcall due to {}", e);
                            throw new RuntimeException(e);
                        }
                    });
            });
    }

    @Override
    public void resetObjectUnsafe(VersionLockedObject<T> object) {
        try {
            object.setObjectUnsafe(getNewInstance());
            object.clearOptimisticVersionUnsafe();
            object.resetStreamViewUnsafe();
            object.version = 0L;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long logUpdate(String smrUpdateFunction, Object[] conflictObject,
                          Object... args) {
        // If we aren't coming from a transactional context,
        // redirect us to a transactional context first.
        if (TransactionalContext.isInTransaction()) {
            // We generate an entry to avoid exposing the serializer to the tx context.
            SMREntry entry = new SMREntry(smrUpdateFunction, args, serializer);
            return TransactionalContext.getCurrentContext()
                    .logUpdate(this, entry, conflictObject);
        }

        // If we aren't in a transaction, we can just write the modification.
        // We need to add the acquired token into the pending upcall list.
        SMREntry smrEntry = new SMREntry(smrUpdateFunction, args, serializer);
        long address = underlyingObject.getStreamViewUnsafe().append(smrEntry, t -> {
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
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <R> R getUpcallResult(long timestamp, Object[] conflictObject) {

        // If we aren't coming from a transactional context,
        // redirect us to a transactional context first.
        if (TransactionalContext.isInTransaction()) {
            return (R) TransactionalContext.getCurrentContext()
                    .getUpcallResult(this, timestamp, conflictObject);
        }

        // Check first if we have the upcall, if we do
        // we can service the request right away.
        if (upcallResults.containsKey(timestamp)) {
            R ret = (R) upcallResults.get(timestamp);
            upcallResults.remove(timestamp);
            return ret == NullValue.NULL_VALUE ? null : ret;
        }

        // if someone took a writelock on the object, we
        // should just wait for it, since the object will
        // have are upcall after...
        if (underlyingObject.isWriteLocked()) {
            underlyingObject.waitOnLock();
            if (upcallResults.containsKey(timestamp)) {
                R ret = (R) upcallResults.get(timestamp);
                upcallResults.remove(timestamp);
                return ret == NullValue.NULL_VALUE ? null : ret;
            }
        }

        // Otherwise we need to sync the object by taking
        // the correct locks.
        underlyingObject.writeReturnVoid(
                (v, obj) -> syncObjectUnsafe(underlyingObject, timestamp));

        if (upcallResults.containsKey(timestamp)) {
            R ret = (R) upcallResults.get(timestamp);
            upcallResults.remove(timestamp);
            return ret == NullValue.NULL_VALUE ? null : ret;
        }

        // The version is already ahead, but we don't have the result.
        // The only way to get the correct result
        // of the upcall would be to rollback. For now, we throw an exception
        // since this is generally not expected. --- and probably a bug if it happens.
        throw new RuntimeException("Attempted to get the result " +
                "of an upcall@" + timestamp + " but we are @"
                + underlyingObject.getVersionUnsafe() +
                " and we don't have a copy");
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
        long sleepTime = 1L;
        while (true) {
            try {
                rt.getObjectsView().TXBegin();
                R ret = txFunction.get();
                rt.getObjectsView().TXEnd();
                return ret;
            } catch (Exception e) {
                log.debug("Transactional function aborted due to {}, retrying after {} msec", e, sleepTime);
                try {Thread.sleep(sleepTime); }
                catch (Exception ex) {}
                sleepTime = min(sleepTime * 2L, 1000L);
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


    /**
     * Get a new version locked object, with a fresh stream view.
     * @return  A new version locked object.
     */
    @SuppressWarnings("unchecked")
    private VersionLockedObject<T> getNewVersionLockedObject() {
        try {
            return new VersionLockedObject<T>(getNewInstance(),
                    -1L,
                    rt.getStreamsView().get(streamID),
                    getUpcallTargetMap(), getUndoRecordTargetMap(),
                    getUndoTargetMap());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Get a new instance of the real underlying object.
     *
     * @return An instance of the real underlying object
     * @throws InstantiationException   If the object cannot be instantiated
     * @throws IllegalAccessException   If for some reason the object class was not public
     */
    @SuppressWarnings("unchecked")
    private T getNewInstance()
            throws InstantiationException, IllegalAccessException {
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
        if (ret instanceof ICorfuSMRProxyWrapper) {
            ((ICorfuSMRProxyWrapper<T>) ret).setProxy$CORFUSMR(this);
        }
        return ret;
    }
}

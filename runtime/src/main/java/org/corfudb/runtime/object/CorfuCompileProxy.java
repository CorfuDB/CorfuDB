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
import org.corfudb.util.serializer.Serializers;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;

/**
 * Created by mwei on 11/11/16.
 */
@Slf4j
public class CorfuCompileProxy<T> implements ICorfuSMRProxy<T> {

    long versionNum = -1L;
    int transactionVersionNum = -1;
    Object lastTransactionalUpcallResult = null;

    T underlyingObject;

    CorfuRuntime rt;
    StreamView sv;
    Class<T> type;
    Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap;

    Set<Long> pendingUpcalls;

    // This enum is necessary because null cannot be inserted
    // into a ConcurrentHashMap.
    enum NullValue {
        NULL_VALUE
    }
    Map<Long, Object> upcallResults;

    StampedLock lock;
    StampedLock txLock;

    public CorfuCompileProxy(CorfuRuntime rt, StreamView sv, Class<T> type,
                             Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap) {
        this.rt = rt;
        this.sv = sv;
        this.type = type;
        this.upcallTargetMap = upcallTargetMap;
        this.pendingUpcalls = new ConcurrentSet<>();
        this.upcallResults = new ConcurrentHashMap<>();

        this.lock = new StampedLock();
        this.txLock = new StampedLock();

        try {
            underlyingObject = type.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void rollback(AbstractTransactionalContext txContext) {
        // Well, it's always safe to just create the object from scratch.
        try {
            underlyingObject = type.newInstance();
            rawAccess(versionNum, e -> null, true);
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
                if (!TransactionalContext.getCurrentContext()
                        .markProxyRead(this)) {
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

                    // When the transaction commits, we can unlock.
                    TransactionalContext.getCurrentContext()
                            .addPostCommitAction((x, ts) -> {
                                // If not a read-only transaction, update the version.
                                if (ts != -1L) {
                                    versionNum = ts;
                                }
                                txLock.unlock(ls);});

                    // When the transaction aborts, we must roll back and then unlock.
                    TransactionalContext.getCurrentContext()
                            .addPostAbortAction(x -> {
                                rollback(x);
                                txLock.unlock(ls);
                            });

                    // Make sure that our version is equal to the
                    // firstReadTimestamp, to guarantee snapshot isolation.
                    long firstRead = TransactionalContext.getCurrentContext()
                            .getFirstReadTimestamp();

                    if (firstRead != versionNum) {
                        // Okay, several things can happen here as we attempt
                        // to sync to the firstReadTimestamp

                        // The easy case is that we're behind. So all we need
                        // to do is sync forward.
                        if (versionNum < TransactionalContext.getCurrentContext()
                                .getFirstReadTimestamp()) {
                            rawAccess(timestamp, e -> null, true);
                            // Explicitly set our version number to the firstRead
                            // So we don't sync again.
                            versionNum = firstRead;
                        }
                        // The hard case is if we're ahead. For now, we'll just
                        // dump our state and then sync forward.
                        else {
                            try {
                                underlyingObject = type.newInstance();
                                rawAccess(timestamp, e -> null, true);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }

                    // Now set the transaction version number to -1
                    transactionVersionNum = -1;
                }

                // Okay, so now we need to check if we have pending updates
                // to this object we need to apply. If so, we apply them...
                Arrays.stream(TransactionalContext.getCurrentContext()
                        .readTransactionLog(transactionVersionNum + 1))
                        .forEachOrdered(x -> {
                            try {
                                ICorfuSMRUpcallTarget<T> target = upcallTargetMap.get(x.getSMRMethod());
                                if (target == null) {throw new Exception("Unknown upcall " + x.getSMRMethod());}
                                lastTransactionalUpcallResult = target.upcall(underlyingObject,
                                        x.getSMRArguments());
                            }
                            catch (Exception e) {
                                log.error("Error: Couldn't execute upcall due to {}", e);
                                throw new RuntimeException(e);
                            }
                        });

                if (accessMethod == null) {
                    // Because transactions force a single thread,
                    // Recording just the last upcall is sufficient.
                    return (R) lastTransactionalUpcallResult;
                }
                else {
                    return accessMethod.access(underlyingObject);
                }
            }


        long ls = txLock.readLock();
        try {
            return rawAccess(timestamp, accessMethod, false);
        } finally {
            txLock.unlock(ls);
        }
    }


    /**
     * Access the raw state of the object, ignoring any transactions.
     * If accessMethod is null, it is expected that
     * the result of the upcall at timestamp is desired.
     *
     * @param timestamp    The timestamp to access the object at.
     * @param accessMethod The method to execute when accessing an object.
     * @param noLock       True, if locking is not required.
     * @return
     */
    private <R> R rawAccess(long timestamp, ICorfuSMRAccess<R, T> accessMethod, boolean noLock) {
        // If we want the most recent
        // version, first figure out what the most recent version is...
        if (timestamp == Long.MAX_VALUE) {
            TokenResponse tr =
                    rt.getSequencerView().nextToken(Collections.singleton(sv.getStreamID()), 0);
            timestamp = tr.getToken();
        }

        // The version is already ahead.
        if (accessMethod == null && versionNum >= timestamp) {
            long ls = lock.tryOptimisticRead();

            // This is always safe, since we're good if we have
            // the result.
            if (upcallResults.containsKey(timestamp)) {
                R ret = (R) upcallResults.get(timestamp);
                upcallResults.remove(timestamp);
                return ret == NullValue.NULL_VALUE ? null : ret;
            }

            // IF we don't have the result, we need to wait if
            // the optimistic lock failed, in case a log read is in progress.
            if (!lock.validate(ls)) {
                ls = lock.readLock();
                try {
                    if (upcallResults.containsKey(timestamp)) {
                        R ret = (R) upcallResults.get(timestamp);
                        upcallResults.remove(timestamp);
                        return ret == NullValue.NULL_VALUE ? null : ret;
                    }
                } finally {
                    lock.unlock(ls);
                }
            }

            // The version is already ahead, but we don't have the result.
            // The only way to get the correct result
            // of the upcall would be to rollback. For now, we throw an exception
            // since this is generally not expected.
            throw new RuntimeException("Attempted to get the result of an upcall@" + timestamp +" which" +
                    "was already executed and we don't have a copy");
        }

        // We have the version requested, so optimistically read
        if (versionNum == timestamp) {
            long ls = lock.tryOptimisticRead();
            R ret = accessMethod.access(underlyingObject);
            // retry with full readLock if we have failed.
            if (!noLock && !lock.validate(ls)) {
                ls = lock.readLock();
                try {
                    return accessMethod.access(underlyingObject);
                } finally {
                    lock.unlock(ls);
                }
            }
            return ret;
        }

        // We don't have the version requested.
        // Now, create the version requested by applying each update found
        // in sequence. Take the write lock for this operation.
        long ls = noLock ? 0L : lock.writeLock();
        try {
            // Read up to timestamp and apply the updates.
            sv.setLogPointer(versionNum + 1L);
            AtomicReference<R> upcallResult = new AtomicReference<R>();
            final long upcallTimestamp = timestamp;
            Arrays.stream(sv.readTo(timestamp))
                    // Turn this into a flat stream of SMR entries.
                    .filter(m -> m.getType() == DataType.DATA)
                    .filter(m -> m.getPayload(rt) instanceof ISMRConsumable)
                    .map(m -> (ISMRConsumable) m.getPayload(rt))
                    .map(c -> c.getSMRUpdates(sv.getStreamID()))
                    .flatMap(List::stream)
                    // Apply each entry, in order into the underlyingObject.
                    .forEachOrdered(l -> {try {
                        ICorfuSMRUpcallTarget<T> target = upcallTargetMap.get(l.getSMRMethod());
                        if (target == null) {throw new Exception("Unknown upcall " + l.getSMRMethod());}
                        R res = (R) target.upcall(underlyingObject, l.getSMRArguments());
                        versionNum = l.getEntry().getGlobalAddress();
                        if (l.getEntry().getGlobalAddress() == upcallTimestamp) {
                            upcallResult.set(res);
                            pendingUpcalls.remove(upcallTimestamp);
                        } else {
                            if (pendingUpcalls.contains(versionNum)) {
                                pendingUpcalls.remove(versionNum);
                                upcallResults.put(versionNum, res == null ? NullValue.NULL_VALUE : res);
                            }
                        }
                    } catch (Exception e) {
                        log.error("Error: Couldn't execute upcall due to {}", e);
                        throw new RuntimeException(e);
                    }});
            // Access the object state.
            return accessMethod == null ? upcallResult.get() : accessMethod.access(underlyingObject);
        } finally {
            if (!noLock) {lock.unlock(ls);}
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
                    .bufferUpdate(sv.getStreamID(), smrUpdateFunction, args, Serializers.JAVA);
        }

        // If we aren't in a transaction, we can just write the modification.
        // We need to add the acquired token into the pending upcall list.
        SMREntry smrEntry = new SMREntry(smrUpdateFunction, args, Serializers.JAVA);
        long address = sv.acquireAndWrite(smrEntry, t -> {
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
        return sv.getStreamID();
    }
}

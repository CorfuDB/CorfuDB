package org.corfudb.runtime.object.transactions;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.CorfuSMRObjectProxy;
import org.corfudb.runtime.object.ICorfuObject;
import org.corfudb.util.serializer.Serializers;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Created by mwei on 9/19/16.
 */
@Slf4j
public class LockingTransactionalContext extends AbstractTransactionalContext {

    AtomicInteger updateCounter;
    @Getter
    Map<CorfuSMRObjectProxy, LockingTransactionalContext.TransactionalObjectData> objectMap;
    @Getter
    private boolean firstReadTimestampSet = false;
    @Setter
    Object[] writeSet;

    public long getFirstReadTimestamp() {
        return getFirstTimestamp().getToken() - 1L;
    }

    @Override
    public boolean transactionRequiresReadLock() { return true; }

    /**
     * The timestamp of the first read in the system.
     *
     * @return The timestamp of the first read object, which may be null.
     */
    @Getter(lazy = true)
    private final TokenResponse firstTimestamp = fetchFirstTimestamp();

    public LockingTransactionalContext(CorfuRuntime runtime) {
        super(runtime);
        this.objectMap = new ConcurrentHashMap<>();
        this.updateCounter = new AtomicInteger();
    }

    /**
     * Check if there was nothing to write.
     *
     * @return Return true, if there was no write set.
     */
    public boolean hasNoWriteSet() {
        for (LockingTransactionalContext.TransactionalObjectData od : objectMap.values()) {
            if (od.bufferedWrites.size() > 0) return false;
        }
        return true;
    }

    /**
     * Get the first timestamp for this transaction.
     *
     * @return The first timestamp to be used for this transaction.
     */
    public synchronized TokenResponse fetchFirstTimestamp() {
        firstReadTimestampSet = true;
        Set<UUID> writeIds = Arrays.stream(writeSet)
                .map(ICorfuObject.class::cast)
                .map(ICorfuObject::getStreamID)
                .collect(Collectors.toSet());
        TokenResponse token = runtime.getSequencerView()
                .nextToken(writeIds, 1);
        log.trace("Set first read timestamp for tx {} to {}", transactionID, token);
        return token;
    }

    /**
     * Compute and write a TXEntry for this transaction to insert into the log.
     *
     * @return A TXEntry which represents this transactional context.
     */
    public MultiObjectSMREntry getEntry() {
        Map<UUID, MultiSMREntry> entryMap = new HashMap<>();
        objectMap.entrySet().stream()
                .forEach(x -> entryMap.put(x.getKey().getSv().getStreamID(),
                        new MultiSMREntry(x.getValue().bufferedWrites)));
        return new MultiObjectSMREntry(entryMap);
    }


    @Override
    public <T> void resetObject(CorfuSMRObjectProxy<T> proxy) {
        objectMap
                .compute(proxy, (k, v) ->
                {
                    LockingTransactionalContext.TransactionalObjectData<T> data = v;
                    if (v == null) {
                        data = new LockingTransactionalContext.TransactionalObjectData<>(proxy);
                    }

                    data.objectIsRead = false;

                    data.bufferedWrites.clear();
                    data.nextCloneIsReset = true;
                    return data;
                });
    }

    /**
     * Commit a transaction into this transaction by merging the read/write sets.
     *
     * @param tc The transaction to merge.
     */
    @SuppressWarnings("unchecked")
    public void addTransaction(AbstractTransactionalContext tc) {
        throw new UnsupportedOperationException("Locking transaction doesn't support nesting yet");
    }

    @Override
    public void commitTransaction() throws TransactionAbortedException {
        MultiObjectSMREntry entry = getEntry();
        try {
            runtime.getStreamsView().writeAt(getFirstTimestamp(), entry.getEntryMap().keySet(), entry);
        } catch (OverwriteException oe) {
            throw new TransactionAbortedException();
        }
    }

    /**
     * Add to the read set
     *
     * @param proxy     The SMR Object proxy to get an object for writing.
     * @param SMRMethod
     * @param result    @return          An object for writing.
     */
    @Override
    public <T> void addReadSet(CorfuSMRObjectProxy<T> proxy, String SMRMethod, Object result) {

    }

    /**
     * Open an object for reading. The implementation will avoid creating a copy of the object
     * if it has not already been done.
     *
     * @param proxy The SMR Object proxy to get an object for reading.
     * @param <T>   The type of object to get for reading.
     * @return An object for reading.
     */
    @SuppressWarnings("unchecked")
    public <T> T getObjectRead(CorfuSMRObjectProxy<T> proxy) {
        return (T) objectMap
                .computeIfAbsent(proxy, x -> new LockingTransactionalContext.TransactionalObjectData<>(proxy))
                .readObject();
    }

    /**
     * Open an object for writing. For opacity, the implementation will create a clone of the
     * object.
     *
     * @param proxy The SMR Object proxy to get an object for writing.
     * @param <T>   The type of object to get for writing.
     * @return An object for writing.
     */
    @SuppressWarnings("unchecked")
    public <T> T getObjectWrite(CorfuSMRObjectProxy<T> proxy) {
        return (T) objectMap
                .computeIfAbsent(proxy, x -> new LockingTransactionalContext.TransactionalObjectData<>(proxy))
                .writeObject();
    }

    /**
     * Open an object for reading and writing. For opacity, the implementation will create a clone of the
     * object.
     *
     * @param proxy The SMR Object proxy to get an object for writing.
     * @param <T>   The type of object to get for writing.
     * @return An object for writing.
     */
    @SuppressWarnings("unchecked")
    public <T> T getObjectReadWrite(CorfuSMRObjectProxy<T> proxy) {
        return (T) objectMap
                .computeIfAbsent(proxy, x -> new LockingTransactionalContext.TransactionalObjectData<>(proxy))
                .readWriteObject();
    }

    /**
     * Check if the object is cloned.
     *
     * @param proxy The SMR Object proxy to get an object for writing.
     * @param <T>   The type of object to get for writing.
     * @return An object for writing.
     */
    @SuppressWarnings("unchecked")
    public <T> boolean isObjectCloned(CorfuSMRObjectProxy<T> proxy) {
        return objectMap.containsKey(proxy) && objectMap.get(proxy).objectIsCloned();
    }

    @SuppressWarnings("unchecked")
    class TransactionalObjectData<T> {

        CorfuSMRObjectProxy<T> proxy;
        T smrObjectClone;
        long readTimestamp;
        List<SMREntry> bufferedWrites;
        boolean objectIsRead;
        boolean nextCloneIsReset;

        public TransactionalObjectData(CorfuSMRObjectProxy<T> proxy) {
            this.proxy = proxy;
            this.bufferedWrites = new ArrayList<>();
            this.readTimestamp = Long.MIN_VALUE;
            this.objectIsRead = false;
            this.nextCloneIsReset = false;
        }

        public boolean objectIsCloned() {
            return smrObjectClone != null;
        }

        T cloneAndGetObject() {
            if (smrObjectClone == null) {
                log.debug("Cloning SMR object {} due to transactional write.", proxy.getSv().getStreamID());
                if (nextCloneIsReset) {
                    log.trace("SMR object was marked for reset, constructing from scratch.");
                    try {
                        smrObjectClone = proxy.constructSMRObject(null);
                        return smrObjectClone;
                    } catch (Exception ex) {
                        log.warn("Error constructing SMR object", ex);
                    }
                }
                smrObjectClone = (T) Serializers.getSerializer(proxy.getSerializer().getType())
                        .clone(proxy.getSmrObject(), proxy.getRuntime());
            }
            return smrObjectClone;
        }

        public T readObject() {
            if (bufferedWrites.isEmpty()) {
                objectIsRead = true;
            }
            readTimestamp = proxy.getTimestamp();
            return (T) (smrObjectClone == null ? proxy.getSmrObject() : smrObjectClone);
        }

        public T writeObject() {
            //return cloneAndGetObject();
            return (T) (smrObjectClone == null ? proxy.getSmrObject() : smrObjectClone);
        }

        public T readWriteObject() {
            if (bufferedWrites.isEmpty()) {
                objectIsRead = true;
            }
            readTimestamp = proxy.getTimestamp();
            //return cloneAndGetObject();
            return (T) (smrObjectClone == null ? proxy.getSmrObject() : smrObjectClone);
        }
    }

}

package org.corfudb.runtime.object.transactions;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.logprotocol.TXEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.CorfuSMRObjectProxy;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by mwei on 4/4/16.
 */
@Slf4j
public class OptimisticTransactionalContext extends AbstractTransactionalContext {

    AtomicInteger updateCounter;
    @Getter
    Map<CorfuSMRObjectProxy, TransactionalObjectData> objectMap;
    @Getter
    private boolean firstReadTimestampSet = false;
    /**
     * The timestamp of the first read in the system.
     *
     * @return The timestamp of the first read object, which may be null.
     */
    @Getter(lazy = true)
    private final long firstReadTimestamp = fetchFirstTimestamp();

    @Override
    public boolean transactionRequiresReadLock() { return true; }

    public OptimisticTransactionalContext(CorfuRuntime runtime) {
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
        for (TransactionalObjectData od : objectMap.values()) {
            if (od.bufferedWrites.size() > 0) return false;
        }
        if (updateLog.size() > 0) {
            return false;
        }
        return true;
    }

    /**
     * Get the first timestamp for this transaction.
     *
     * @return The first timestamp to be used for this transaction.
     */
    public synchronized long fetchFirstTimestamp() {
        firstReadTimestampSet = true;
        long token = runtime.getSequencerView().nextToken(Collections.emptySet(), 0).getToken();
        log.trace("Set first read timestamp for tx {} to {}", transactionID, token);
        return token;
    }

    /**
     * Compute and write a TXEntry for this transaction to insert into the log.
     *
     * @return A TXEntry which represents this transactional context.
     */
    public TXEntry getEntry() {
        Map<UUID, TXEntry.TXObjectEntry> entryMap = new HashMap<>();
        objectMap.entrySet().stream()
                .forEach(x -> entryMap.put(x.getKey().getSv().getStreamID(),
                        new TXEntry.TXObjectEntry(x.getValue().bufferedWrites, x.getValue().objectIsRead)));

        // new TX stuff.
        updateMap.entrySet().stream()
                .forEach(x -> entryMap.put(x.getKey(),
                        new TXEntry.TXObjectEntry(x.getValue(), false)));
        readProxies
                .forEach(x -> {
                    if (entryMap.containsKey(x.getStreamID())) {
                        entryMap.get(x.getStreamID()).setRead(true);
                    }
                    else {
                        entryMap.put(x.getStreamID(), new TXEntry.TXObjectEntry(Collections.emptyList(),
                                true));
                    }
                });
        return new TXEntry(entryMap, isFirstReadTimestampSet() ? getFirstReadTimestamp() : -1L);
    }

    /**
     * Buffer away an object update, adding it to the write set that will be generated
     * in the resulting TXEntry.
     *
     * @param proxy        The SMR Object proxy to buffer for.
     * @param SMRMethod    The method being called.
     * @param SMRArguments The arguments to that method.
     * @param serializer   The serializer to use.
     * @param <T>          The type of the proxy.
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> void bufferObjectUpdate(CorfuSMRObjectProxy<T> proxy, String SMRMethod,
                                       Object[] SMRArguments, ISerializer serializer, boolean writeOnly) {
        objectMap
                .compute(proxy, (k, v) ->
                {
                    TransactionalObjectData<T> data = v;
                    if (v == null) {
                        data = new TransactionalObjectData<>(proxy);
                    }

                    if (!writeOnly) {
                        data.objectIsRead = true;
                    }
                    data.bufferedWrites.add(new SMREntry(SMRMethod, SMRArguments, serializer));
                    return data;
                });
    }

    @Override
    public <T> void resetObject(CorfuSMRObjectProxy<T> proxy) {
        objectMap
                .compute(proxy, (k, v) ->
                {
                    TransactionalObjectData<T> data = v;
                    if (v == null) {
                        data = new TransactionalObjectData<>(proxy);
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
        if (tc instanceof OptimisticTransactionalContext) {
            ((OptimisticTransactionalContext) tc).getObjectMap().entrySet().stream()
                    .forEach(e -> {
                        if (objectMap.containsKey(e.getKey())) {
                            objectMap.get(e.getKey())
                                    .bufferedWrites.addAll(e.getValue().bufferedWrites);
                        } else {
                            objectMap.put(e.getKey(), e.getValue());
                        }
                    });
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
                .computeIfAbsent(proxy, x -> new TransactionalObjectData<>(proxy))
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
                .computeIfAbsent(proxy, x -> new TransactionalObjectData<>(proxy))
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
                .computeIfAbsent(proxy, x -> new TransactionalObjectData<>(proxy))
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

    @Override
    public void commitTransaction() throws TransactionAbortedException {
        TXEntry entry = getEntry();
        Set<UUID> affectedStreams = entry.getAffectedStreams();
        //TODO:: refactor commitTransaction into here...
        long address = runtime.getStreamsView().write(affectedStreams, entry);
        if (address == -1L) {
            log.debug("Transaction aborted due to sequencer rejecting request");
            getPostAbortActions()
                    .forEach(x -> x.accept(this));
            throw new TransactionAbortedException();
        }
        getPostCommitActions()
                .forEach(x -> x.accept(this, address));
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
            return cloneAndGetObject();
        }

        public T readWriteObject() {
            if (bufferedWrites.isEmpty()) {
                objectIsRead = true;
            }
            readTimestamp = proxy.getTimestamp();
            return cloneAndGetObject();
        }
    }


}

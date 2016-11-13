package org.corfudb.runtime.object.transactions;

import io.netty.util.internal.ConcurrentSet;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.annotations.Accessor;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.CorfuSMRObjectProxy;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRProxy;
import org.corfudb.runtime.view.TransactionStrategy;
import org.corfudb.util.serializer.ISerializer;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Created by mwei on 4/4/16.
 */
public abstract class AbstractTransactionalContext implements AutoCloseable {

    @Getter
    public UUID transactionID;

    /**
     * The runtime used to create this transaction.
     */
    @Getter
    public CorfuRuntime runtime;

    /**
     * The start time of the context.
     */
    @Getter
    @Setter
    public long startTime;

    /**
     * The transaction strategy to employ for this transaction.
     */
    @Getter
    @Setter
    public TransactionStrategy strategy;

    /**
     * Whether or not the tx is doing a first sync and therefore writes should not be
     * redirected.
     *
     * @return Whether or not the TX is in sync.
     */
    @Getter
    @Setter
    public boolean inSyncMode;

    AbstractTransactionalContext(CorfuRuntime runtime) {
        transactionID = UUID.randomUUID();
        this.runtime = runtime;
    }

    abstract public long getFirstReadTimestamp();

    /**
     * Check if there was nothing to write.
     *
     * @return Return true, if there was no write set.
     */
    abstract public boolean hasNoWriteSet();

    public <T> void bufferObjectUpdate(CorfuSMRObjectProxy<T> proxy, String SMRMethod,
                                       Object[] SMRArguments, ISerializer serializer, boolean writeOnly) {
    }

    /** New tx methods */

    // We keep a list of read proxies, because multiple proxies
    // may be subscribed to the same stream.
    @Getter
    Set<ICorfuSMRProxy> readProxies = new HashSet<>();

    @Getter
    Map<UUID, List<SMREntry>> updateMap = new HashMap<>();

    @Getter
    List<SMREntry> updateLog = new LinkedList<>();

    @Getter
    Set<BiConsumer<AbstractTransactionalContext, Long>> postCommitActions = new HashSet<>();

    @Getter
    Set<Consumer<AbstractTransactionalContext>> postAbortActions = new HashSet<>();

    public long bufferUpdate(UUID stream, String SMRMethod, Object[] SMRArguments, ISerializer serializer) {
        updateMap.putIfAbsent(stream, new LinkedList<>());
        SMREntry entry = new SMREntry(SMRMethod, SMRArguments, serializer);
        //future optimization might not keep two redundant structures.
        updateMap.get(stream).add(entry);
        updateLog.add(entry);
        return updateLog.size() - 1;
    }

    public <T> boolean markProxyRead(ICorfuSMRProxy<T> proxy) {
        if (readProxies.contains(proxy)){
            return true;
        }
        readProxies.add(proxy);
        setProxyPointer(proxy, 0);
        return false;
    }

    Map<ICorfuSMRProxy<?>, Integer> proxyAddressPointers = new HashMap<>();

    public void setProxyPointer(ICorfuSMRProxy<?> proxy, Integer pointer) {
        proxyAddressPointers.put(proxy, pointer);
    }

    public Integer getProxyPointer(ICorfuSMRProxy<?> proxy) {
        return proxyAddressPointers.get(proxy);
    }


    public void addPostCommitAction(BiConsumer<AbstractTransactionalContext, Long> action) {
        postCommitActions.add(action);
    }
    public void addPostAbortAction(Consumer<AbstractTransactionalContext> action) {
        postAbortActions.add(action);
    }

    public SMREntry[] readTransactionLog(UUID streamID, int readFrom) {
        if (!updateMap.containsKey(streamID)) {return new SMREntry[0]; }
        ArrayList<SMREntry> list = new ArrayList<>();
        for (int i = readFrom; i < updateMap.get(streamID).size(); i++){
            list.add(updateMap.get(streamID).get(i));
        }
        return list.toArray(new SMREntry[list.size()]);
    }

    public int getTransactionLogSize(UUID streamID) {
        if (!updateMap.containsKey(streamID)) {return 0; }
        return updateMap.get(streamID).size();
    }
    /** */

    abstract public <T> void resetObject(CorfuSMRObjectProxy<T> proxy);

    abstract public void addTransaction(AbstractTransactionalContext tc);

    /**
     * Add to the read set
     *
     * @param proxy The SMR Object proxy to get an object for writing.
     * @param <T>   The type of object to get for writing.
     * @return An object for writing.
     */
    abstract public <T> void addReadSet(CorfuSMRObjectProxy<T> proxy, String SMRMethod, Object result);

    /**
     * Open an object for reading. The implementation will avoid creating a copy of the object
     * if it has not already been done.
     *
     * @param proxy The SMR Object proxy to get an object for reading.
     * @param <T>   The type of object to get for reading.
     * @return An object for reading.
     */
    @SuppressWarnings("unchecked")
    abstract public <T> T getObjectRead(CorfuSMRObjectProxy<T> proxy);

    /**
     * Open an object for writing. For opacity, the implementation will create a clone of the
     * object.
     *
     * @param proxy The SMR Object proxy to get an object for writing.
     * @param <T>   The type of object to get for writing.
     * @return An object for writing.
     */
    @SuppressWarnings("unchecked")
    abstract public <T> T getObjectWrite(CorfuSMRObjectProxy<T> proxy);

    /**
     * Open an object for reading and writing. For opacity, the implementation will create a clone of the
     * object.
     *
     * @param proxy The SMR Object proxy to get an object for writing.
     * @param <T>   The type of object to get for writing.
     * @return An object for writing.
     */
    @SuppressWarnings("unchecked")
    abstract public <T> T getObjectReadWrite(CorfuSMRObjectProxy<T> proxy);

    /**
     * Check if the object is cloned.
     *
     * @param proxy The SMR Object proxy to get an object for writing.
     * @param <T>   The type of object to get for writing.
     * @return An object for writing.
     */
    @SuppressWarnings("unchecked")
    public <T> boolean isObjectCloned(CorfuSMRObjectProxy<T> proxy) {
        return false;
    }

    public boolean transactionRequiresReadLock() { return false; }


    public void commitTransaction() throws TransactionAbortedException {
    }

    @Override
    public void close() {

    }
}

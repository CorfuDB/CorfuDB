package org.corfudb.runtime.object.transactions;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.CorfuSMRObjectProxy;
import org.corfudb.runtime.view.TransactionStrategy;
import org.corfudb.util.serializer.Serializers;

import java.util.UUID;

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
                                       Object[] SMRArguments, Serializers.SerializerType serializer, boolean writeOnly) {
    }

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

    @Override
    public void close() {

    }
}

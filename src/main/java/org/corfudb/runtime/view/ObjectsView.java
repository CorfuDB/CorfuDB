package org.corfudb.runtime.view;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.TXEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.CorfuSMRObjectProxy;
import org.corfudb.runtime.object.ISMRInterface;
import org.corfudb.runtime.object.TransactionalContext;

import java.util.UUID;
import java.util.function.Supplier;

/**
 * A view of the objects inside a Corfu instance.
 * Created by mwei on 1/7/16.
 */
@Slf4j
public class ObjectsView extends AbstractView {


    public ObjectsView(CorfuRuntime runtime) {
        super(runtime);
    }

    /** Gets a view of an object in the Corfu instance.
     *
     * @param streamID      The stream that the object should be read from.
     * @param type          The type of the object that should be opened.
     *                      If the type implements ICorfuSMRObject or implements an interface which implements
     *                      ISMRInterface, Accessors, Mutator and MutatorAccessor annotations will be respected.
     *                      Otherwise, the entire object will be wrapped around SMR and it will be assumed that
     *                      all methods are MutatorAccessors.
     * @param <T>           The type of object to return.
     * @return              Returns a view of the object in a Corfu instance.
     */
    public <T> T open(@NonNull UUID streamID, @NonNull Class<T> type) {
        return open(streamID, type, null);
    }

    /** Gets a view of an object in the Corfu instance.
     *
     * @param streamName    The stream that the object should be read from.
     * @param type          The type of the object that should be opened.
     *                      If the type implements ICorfuSMRObject or implements an interface which implements
     *                      ISMRInterface, Accessors, Mutator and MutatorAccessor annotations will be respected.
     *                      Otherwise, the entire object will be wrapped around SMR and it will be assumed that
     *                      all methods are MutatorAccessors.
     * @param <T>           The type of object to return.
     * @return              Returns a view of the object in a Corfu instance.
     */
    public <T> T open(@NonNull String streamName, @NonNull Class<T> type) {
        return open(CorfuRuntime.getStreamID(streamName), type);
    }

    /** Gets a view of an object in the Corfu instance.
     *
     * @param streamName    The stream that the object should be read from.
     * @param type          The type of the object that should be opened.
     *                      If the type implements ICorfuSMRObject or implements an interface which implements
     *                      ISMRInterface, Accessors, Mutator and MutatorAccessor annotations will be respected.
     *                      Otherwise, the entire object will be wrapped around SMR and it will be assumed that
     *                      all methods are MutatorAccessors.
     * @param overlay       The ISMRInterface to overlay on top of the object, if provided.
     * @param <T>           The type of object to return.
     * @return              Returns a view of the object in a Corfu instance.
     */
    @SuppressWarnings("unchecked")
    public <T, R extends ISMRInterface> T open(@NonNull String streamName, @NonNull Class<T> type, Class<R> overlay) {
        return open(CorfuRuntime.getStreamID(streamName), type, overlay);
    }

    /** Gets a view of an object in the Corfu instance.
     *
     * @param streamID      The stream that the object should be read from.
     * @param type          The type of the object that should be opened.
     *                      If the type implements ICorfuSMRObject or implements an interface which implements
     *                      ISMRInterface, Accessors, Mutator and MutatorAccessor annotations will be respected.
     *                      Otherwise, the entire object will be wrapped around SMR and it will be assumed that
     *                      all methods are MutatorAccessors.
     * @param overlay       The ISMRInterface to overlay on top of the object, if provided.
     * @param <T>           The type of object to return.
     * @return              Returns a view of the object in a Corfu instance.
     */
    @SuppressWarnings("unchecked")
    public <T, R extends ISMRInterface> T open(@NonNull UUID streamID, @NonNull Class<T> type, Class<R> overlay) {
        StreamView sv = runtime.getStreamsView().get(streamID);
        return CorfuSMRObjectProxy.getProxy(type, overlay, sv, runtime);
    }

    /** Begins a transaction on the current thread.
     *  Modifications to objects will not be visible
     *  to other threads or clients until TXEnd is called.
     */
    public void TXBegin() {
        TransactionalContext context = TransactionalContext.newContext();
        log.trace("Entering transactional context {}.", context.getTransactionID());
    }

    /** Aborts a transaction on the current thread.
     * Modifications to objects in the current transactional
     * context will be discarded.
     */
    public void TXAbort() {
        TransactionalContext context = TransactionalContext.removeContext();
        if (context == null)
        {
            log.warn("Attempted to abort a transaction, but no transaction active!");
        }
        else {
            log.trace("Aborting transactional context {}.", context.getTransactionID());
        }
    }

    /** Query whether a transaction is currently running.
     *
     * @return  True, if called within a transactional context,
     *          False, otherwise.
     */
    public boolean TXActive() {
        return TransactionalContext.isInTransaction();
    }

    /** End a transaction on the current thread.
     *  @throws TransactionAbortedException      If the transaction could not be executed successfully.
     */
    public void TXEnd()
        throws TransactionAbortedException
    {
        TransactionalContext context = TransactionalContext.getCurrentContext();
        if (context == null)
        {
            log.warn("Attempted to end a transaction, but no transaction active!");
        }
        else {
            log.trace("Exiting (committing) transactional context {}.", context.getTransactionID());
            TXEntry entry = context.getEntry();
            long address = runtime.getStreamsView().write(entry.getAffectedStreams(), entry);
            TransactionalContext.removeContext();
            log.trace("TX entry {} written at address {}", entry, address);
            //now check if the TX will be an abort...
            if (entry.isAborted(runtime, address))
            {
                throw new TransactionAbortedException();
            }
        }
    }

    /** Executes the supplied function transactionally.
     *
     * @param txFunction                        The function to execute transactionally.
     * @param <T>                               The return type of the function to execute.
     * @return                                  The return value of the function.
     * @throws TransactionAbortedException      If the transaction could not be executed successfully.
     */
    public <T> T executeTX(Supplier<T> txFunction)
        throws TransactionAbortedException
    {
        TXBegin();
        T result = txFunction.get();
        TXEnd();
        return result;
    }

}

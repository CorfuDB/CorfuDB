package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatchers;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.Accessor;
import org.corfudb.runtime.object.CorfuSMRObjectProxy;
import org.corfudb.runtime.object.Mutator;
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
    @SuppressWarnings("unchecked")
    public <T> T open(UUID streamID, Class<T> type) {
        StreamView sv = runtime.getStreamsView().get(streamID);
        return CorfuSMRObjectProxy.getProxy(type, sv);
    }

    /** Begins a transaction on the current thread.
     *  Modifications to objects will not be visible
     *  to other threads or clients until endTX is called.
     */
    public void beginTX() {
        TransactionalContext context = TransactionalContext.newContext();
        log.trace("Entering transactional context {}.", context.getTransactionID());
    }

    /** Aborts a transaction on the current thread.
     * Modifications to objects in the current transactional
     * context will be discarded.
     */
    public void abortTX() {
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
    public boolean activeTX() {
        return TransactionalContext.isInTransaction();
    }

    /** End a transaction on the current thread.
     *  @throws TransactionAbortedException      If the transaction could not be executed successfully.
     */
    public void endTX()
        throws TransactionAbortedException
    {
        TransactionalContext context = TransactionalContext.getCurrentContext();
        if (context == null)
        {
            log.warn("Attempted to end a transaction, but no transaction active!");
        }
        else {
            log.trace("Exiting transactional context {}.", context.getTransactionID());
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
        beginTX();
        T result = txFunction.get();
        endTX();
        return result;
    }

}

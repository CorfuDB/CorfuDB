package org.corfudb.runtime.smr;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.runtime.view.Serializer;

/**
 * Created by mwei on 5/5/15.
 */
public class OpaqueDeferredTransaction extends DeferredTransaction {

    public OpaqueDeferredTransaction(ICorfuDBInstance instance)
    {
        super(instance);
    }

    /**
     * Execute this command on a specific SMR engine.
     *
     * @param engine The SMR engine to run this command on.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void executeTransaction(ISMREngine engine) {
        //Clone the underlying object
        Object clone = Serializer.copy(engine.getObject());
        engine.getStreamID();
        executingEngine = engine;
        try (TransactionalContext tx = new TransactionalContext(this))
        {
            ITransactionCommand command = getTransaction();
            command.apply(new DeferredTransactionOptions());
        }
        catch (TransactionAbortedException e)
        {
            engine.setObject(clone);
        }
    }
}

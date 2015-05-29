package org.corfudb.runtime.smr;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.view.Serializer;

/**
 * Created by mwei on 5/5/15.
 */
public class OpaqueDeferredTransaction extends DeferredTransaction {

    public OpaqueDeferredTransaction(CorfuDBRuntime runtime)
    {
        super(runtime);
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
            if (!command.apply(new DeferredTransactionOptions())) {
                engine.setObject(clone);
            }
        }
    }
}

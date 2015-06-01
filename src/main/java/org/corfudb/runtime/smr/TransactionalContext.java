package org.corfudb.runtime.smr;

import org.apache.zookeeper.Transaction;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.stream.ITimestamp;

import java.io.Closeable;

/**
 * This class implements the thread-local transactional context
 * used for determining whether or not an object is part of a transaction.
 *
 * Created by mwei on 5/28/15.
 */
public class TransactionalContext implements AutoCloseable {

    /**
     * The currently executing transaction on this thread.
     */
    public static ThreadLocal<ITransaction> currentTX = ThreadLocal.withInitial(() -> null);

    /**
     * Create a new transactional context.
     * @param transaction   The transaction to execute this context under.
     */
    public TransactionalContext(ITransaction transaction)
    {
        currentTX.set(transaction);
    }


    /**
     * Create a new local transactional context.
     * @param engine        The engine to redirect updates to.
     * @param ts            The timestamp this context occurs at.
     * @param cdr           The CorfuDBRuntime to pass through.
     */
    public TransactionalContext(ISMREngine engine, ITimestamp ts, CorfuDBRuntime cdr)
    {
        currentTX.set(new LocalTransaction(engine, ts, cdr));
    }

    /**
     * Get the currently executing transaction.
     * @return  The currently executing transaction.
     */
    public ITransaction getTX() {
        return currentTX.get();
    }

    /**
     * Closes this transactional context, releasing the currentTX held by the thread local.
     */
    @Override
    public void close() {
        currentTX.set(null);
    }
}

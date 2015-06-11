package org.corfudb.runtime.smr;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Stack;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * This class implements the thread-local transactional context
 * used for determining whether or not an object is part of a transaction.
 *
 * Created by mwei on 5/28/15.
 */
public class TransactionalContext implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TransactionalContext.class);

    /**
     * The currently executing transaction on this thread.
     */
    public static ThreadLocal<Deque<ITransaction>> currentTX =
            ThreadLocal.withInitial(ArrayDeque<ITransaction>::new);

    /**
     * Get the currently executing transaction.
     * @return  The currently executing transaction.
     */
    public static ITransaction getTX() {
        return currentTX.get().peekFirst();
    }

    /**
     * Create a new transactional context.
     * @param transaction   The transaction to execute this context under.
     */
    public TransactionalContext(ITransaction transaction)
    {
        currentTX.get().addFirst(transaction);
    }


    /**
     * Create a new transactional context.
     * @param engine        The engine to redirect updates to.
     * @param ts            The timestamp this context occurs at.
     * @param cdr           The CorfuDBRuntime to pass through.
     */
    public TransactionalContext(ISMREngine engine, ITimestamp ts, ICorfuDBInstance cdr, Class<? extends ITransaction> txType)
    {
        try {
            currentTX.get().addFirst(txType.getConstructor(ISMREngine.class, ITimestamp.class, ICorfuDBInstance.class)
                    .newInstance(engine, ts, cdr));
        } catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a new transactional context with a result buffer
     * @param engine        The engine to redirect updates to.
     * @param ts            The timestamp this context occurs at.
     * @param res           The timestamp to put any mutations at.
     * @param cdr           The CorfuDBRuntime to pass through.
     */
    public TransactionalContext(ISMREngine engine, ITimestamp ts, ITimestamp res, ICorfuDBInstance cdr, Class<? extends ITransaction> txType)
    {
        try {
            currentTX.get().addFirst(txType.getConstructor(ISMREngine.class, ITimestamp.class, ITimestamp.class, ICorfuDBInstance.class)
                    .newInstance(engine, ts, res, cdr));
        } catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Closes this transactional context, releasing the currentTX held by the thread local.
     */
    @Override
    public void close() {
        if (currentTX.get().peekFirst() instanceof LocalTransaction)
        {
            log.info("Proposing a locally executed transaction at " + ((LocalTransaction) currentTX.get().peekFirst()).timestamp);
            try {
                currentTX.get().peekFirst().propose();
            }
            catch (Exception e)
            {
                log.warn("Failed to propose local TX...");
                throw new RuntimeException(e);
            }
        }
        currentTX.get().removeFirst();
    }
}

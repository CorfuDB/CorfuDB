package org.corfudb.runtime.smr;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Stack;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * This class implements the thread-local transactional context
 * used for determining whether or not an object is part of a transaction.
 *
 * Created by mwei on 5/28/15.
 */
@Slf4j
public class TransactionalContext implements AutoCloseable {

    /**
     * The currently executing transaction on this thread.
     */
    public static ThreadLocal<Deque<ITransaction>> currentTX =
            ThreadLocal.withInitial(ArrayDeque<ITransaction>::new);

    public static ConcurrentHashMap<ITimestamp, CompletableFuture<?>> transactionalFutureMap
            = new ConcurrentHashMap<>();

    public static CompletableFuture<?> getTransactionalFuture(ITimestamp ts) {
        return transactionalFutureMap.get(ts);
    }

    public static void setTransactionalFuture(ITimestamp ts, CompletableFuture<?> cf) {
        transactionalFutureMap.put(ts, cf);
    }

    public static ITransaction getCurrentTX() {
        return currentTX.get().peekFirst();
    }

    public TransactionalContext(ITransaction tx)
    {
        currentTX.get().addFirst(tx);
    }

    /**
     * Closes this transactional context, releasing the currentTX held by the thread local.
     */
    @Override
    public void close() {
        currentTX.get().removeFirst();
    }
}

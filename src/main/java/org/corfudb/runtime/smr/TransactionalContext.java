package org.corfudb.runtime.smr;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuDBRuntime;
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
    public static ThreadLocal<Deque<TransactionalContext>> currentTX =
            ThreadLocal.withInitial(ArrayDeque<TransactionalContext>::new);

    public static ConcurrentHashMap<ITimestamp, CompletableFuture<?>> transactionalFutureMap
            = new ConcurrentHashMap<>();

    public static CompletableFuture<?> getTransactionalFuture(ITimestamp ts) {
        return transactionalFutureMap.get(ts);
    }

    public static void setTransactionalFuture(ITimestamp ts, CompletableFuture<?> cf) {
        transactionalFutureMap.put(ts, cf);
    }

    enum TransactionalMode {
        PASS_THROUGH,   // Apply mutations directly to object state
        BUFFER          // Buffer away mutations
    }

    public static TransactionalMode getCurrentTXMode() {
        return currentTX.get().peekFirst().getMode();
    }

    @Getter
    TransactionalMode mode;

    @Getter
    ITimestamp timestamp;

    public TransactionalContext(TransactionalMode mode, ITimestamp ts)
    {
        this.mode = mode;
        currentTX.get().addFirst(this);
    }

    /**
     * Closes this transactional context, releasing the currentTX held by the thread local.
     */
    @Override
    public void close() {
        currentTX.get().removeFirst();
    }
}

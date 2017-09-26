package org.corfudb.runtime.object.transactions;

import java.util.UUID;

import javax.annotation.Nonnull;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.IObjectManager;
import org.corfudb.runtime.object.IStateMachineStream;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.Utils;


/**
 * Represents a transaction. Concrete classes implement the {@link #commit()} and
 * {@link #getStateMachineStream(IObjectManager, IStateMachineStream)} methods, which
 * determine how the transaction commits, and how reads/writes are redirected during the
 * lifetime of the transaction.
 */
@Slf4j
public abstract class AbstractTransaction {

    /**
     * The ID of the transaction. This is used for tracking only, it is
     * NOT recorded in the log.
     */
    @SuppressWarnings("checkstyle:abbreviationaswordinname")
    @Getter
    public UUID transactionID;

    /**
     * The builder used to create this transaction.
     */
    @Getter
    public final TransactionBuilder builder;

    /** The parent transaction, if this transaction is nested. */
    @Getter
    public final AbstractTransaction parent;

    /** The transaction context for this transaction. */
    @Getter
    public final TransactionContext context;

    /**
     * The start time of the context.
     */
    @Getter
    public final long startTime;

    /** Generate a new transaction given a transaction builder and a parent
     * transaction, if present.
     * @param builder   The builder for this transaction.
     * @param context   The context used when this transaction was built.
     */
    AbstractTransaction(@Nonnull TransactionBuilder builder,
                        @Nonnull TransactionContext context) {
        transactionID = UUID.randomUUID();
        startTime = System.currentTimeMillis();
        this.builder = builder;
        this.parent = context.current;
        this.context = context;
    }


    /** Get a state machine stream for the given object, using the current
     * state machine stream.
     * @param manager   The manager for the object.
     * @param current   The current state machine stream in use for the object.
     * @return          A state machine stream, which enables reads/writes over this
     *                  transaction.
     */
    public abstract IStateMachineStream getStateMachineStream(@Nonnull IObjectManager manager,
                                                          @Nonnull IStateMachineStream current);

    /** Commit this transaction.
     *
     * @return The address this transaction was committed at.
     * @throws TransactionAbortedException  If the transaction was aborted and not committed
     *                                      into the log.
     */
    public long commit() throws TransactionAbortedException {
        return Address.NOWRITE;
    }

    /**
     * Forcefully abort the transaction.
     */
    public void abort(TransactionAbortedException ae) {
        AbstractTransaction.log.debug("abort[{}]", this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "TX[" + Utils.toReadableId(transactionID) + "]";
    }
}
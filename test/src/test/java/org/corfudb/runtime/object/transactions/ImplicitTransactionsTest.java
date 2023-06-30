package org.corfudb.runtime.object.transactions;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.object.TransactionalObject;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ImplicitTransactionsTest extends AbstractTransactionsTest{

    @Override
    public void TXBegin() {
        OptimisticTXBegin();
    }


    /**
     * Implicit transaction should not be nested. All implicit Tx
     * end up in TXExecute, so here we execute isInNestedTransaction
     * in TXExecute to check that we are indeed not in a nested tx.
     *
     */
    @Test
    public void implicitTransactionalMethodAreNotNested() {
        TransactionalObject to = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setTypeToken(new TypeToken<TransactionalObject>() {
                })
                .open();

        TXBegin();
        assertThat(to.isInTransaction()).isTrue();
        assertThat(to.isInNestedTransaction()).isFalse();
        TXEnd();
    }

    /**
     * Implicit transaction should be in a transaction. If we are not
     * in a transaction already, it should create a new transaction,.
     *
     */
    @Test
    public void implicitTransactionalMethodIsInTransaction() {
        TransactionalObject to = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setTypeToken(new TypeToken<TransactionalObject>() {
                })
                .open();

        assertThat(to.isInTransaction()).isTrue();
    }

    /**
     * Runtime exception thrown during "nested" transaction should
     * abort the transaction.
     */
    @Test
    public void runtimeExceptionAbortNestedTransaction() {
        TransactionalObject to = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setTypeToken(new TypeToken<TransactionalObject>() {
                })
                .open();

        TXBegin();
        try {
            to.throwRuntimeException();
            TXEnd();
        } catch (Exception e) {
            assertThat(TransactionalContext.isInTransaction()).isFalse();
        }
    }

}

package org.corfudb.runtime.object.transactions;

import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 11/22/16.
 */
public class WriteAfterWriteTransactionContextTest extends AbstractTransactionContextTest {

    /** In a write after write transaction, concurrent modifications
     * with the same read timestamp should abort.
     */
    @Test
    public void concurrentModificationsCauseAbort()
    {
        t(1, () -> write("k" , "v1"));
        t(1, this::TXBegin);
        t(2, this::TXBegin);
        t(1, () -> get("k"));
        t(2, () -> get("k"));
        t(1, () -> write("k" , "v2"));
        t(2, () -> write("k" , "v3"));
        t(1, this::TXEnd);
        t(2, this::TXEnd)
              .assertThrows()
              .isInstanceOf(TransactionAbortedException.class);

        assertThat(getMap())
                .containsEntry("k", "v2")
                .doesNotContainEntry("k", "v3");
    }

    @Override
    void TXBegin() {
        getRuntime().getObjectsView().TXBuild()
                .setType(TransactionType.WRITE_AFTER_WRITE)
                .begin();
    }
}

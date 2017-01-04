package org.corfudb.runtime.object.transactions;

import com.google.common.reflect.TypeToken;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.ObjectOpenOptions;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.StreamView;
import org.junit.Test;

import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        getRuntime().getObjectsView().setTransactionLogging(true);

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

        // Verify that the transaction that succeeded is written to the transaction stream
        StreamView txStream = getRuntime().getStreamsView().get(ObjectsView.TRANSACTION_STREAM_ID);
        LogData[] txns = txStream.readTo(Long.MAX_VALUE);
        assertThat(txns.length).isEqualTo(1);
        assertThat(txns[0].getLogEntry(getRuntime()).getType()).isEqualTo(LogEntry.LogEntryType.MULTIOBJSMR);

        MultiObjectSMREntry tx1 = (MultiObjectSMREntry)txns[0].getLogEntry(getRuntime());
        assertThat(tx1.getEntryMap().size()).isEqualTo(1);
        MultiSMREntry entryMap = tx1.getEntryMap().entrySet().iterator().next().getValue();
        assertThat(entryMap).isNotNull();
        assertThat(entryMap.getUpdates().size()).isEqualTo(1);
        SMREntry smrEntry = entryMap.getUpdates().get(0);
        Object[] args = smrEntry.getSMRArguments();
        assertThat(smrEntry.getSMRMethod()).isEqualTo("put");
        assertThat((String) args[0]).isEqualTo("k");
        assertThat((String) args[1]).isEqualTo("v2");
    }

    @Override
    void TXBegin() {
        getRuntime().getObjectsView().TXBuild()
                .setType(TransactionType.WRITE_AFTER_WRITE)
                .begin();
    }
}

package org.corfudb.runtime.object.transactions;

import com.google.common.reflect.TypeToken;

import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 11/22/16.
 */
public class WriteAfterWriteTransactionContextTest extends AbstractTransactionContextTest {
    private final String streamName = "test stream";
    private final String txnStreamName = "txn" + streamName;

    private ICorfuTable<String, String> testMap;

    /** In a write after write transaction, concurrent modifications
     * with the same read timestamp should abort.
     */
    @Override
    public void TXBegin() { WWTXBegin(); }


    @Test
    public void concurrentModificationsCauseAbort()
    {
        getRuntime();

        getMap();

        t(1, () -> write("k" , "v1"));
        t(1, this::WWTXBegin);
        t(2, this::WWTXBegin);
        t(1, () -> get("k"));
        t(2, () -> get("k"));
        t(1, () -> write("k" , "v2"));
        t(2, () -> write("k" , "v3"));
        t(1, this::TXEnd);
        t(2, this::TXEnd)
              .assertThrows()
              .isInstanceOf(TransactionAbortedException.class);

        assertThat(getMap().get("k")).isEqualTo("v2");

        IStreamView txStream = getRuntime().getStreamsView()
                .get(CorfuRuntime.getStreamID(txnStreamName));

        List<ILogData> txns = txStream.remainingUpTo(Long.MAX_VALUE);
        assertThat(txns).hasSize(1);
        assertThat(txns.get(0).getLogEntry(getRuntime()).getType()).isEqualTo
            (LogEntry.LogEntryType.MULTIOBJSMR);

        MultiObjectSMREntry tx1 = (MultiObjectSMREntry)txns.get(0).getLogEntry
            (getRuntime());
        assertThat(tx1.getEntryMap().size()).isEqualTo(1);
        MultiSMREntry entryMap = tx1.getEntryMap().entrySet().iterator()
                                                        .next().getValue();
        assertThat(entryMap).isNotNull();
        assertThat(entryMap.getUpdates().size()).isEqualTo(1);
        SMREntry smrEntry = entryMap.getUpdates().get(0);
        Object[] args = smrEntry.getSMRArguments();
        assertThat(smrEntry.getSMRMethod()).isEqualTo("put");
        assertThat((String) args[0]).isEqualTo("k");
        assertThat((String) args[1]).isEqualTo("v2");
    }

    @Override
    public ICorfuTable<String, String> getMap() {
        if (testMap == null) {
            Object obj = getRuntime().getObjectsView().build()
                .setStreamName(streamName)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamTags(getRuntime().getStreamID(txnStreamName))
                .open();
            testMap = (ICorfuTable<String, String>) obj;
        }
       return testMap;
    }
}

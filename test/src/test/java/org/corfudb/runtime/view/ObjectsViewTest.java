package org.corfudb.runtime.view;

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
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 2/18/16.
 */
public class ObjectsViewTest extends AbstractViewTest {

    @Test
    public void canAbortNoTransaction()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime();
        r.getObjectsView().TXAbort();
    }

    @Test
    public void abortedTransactionDoesNotConflict()
            throws Exception {
        final String mapA = "map a";
        final String streamA = "streamA";
        CorfuRuntime r = getDefaultRuntime();

        ICorfuTable<String, String> map = getDefaultRuntime().getObjectsView()
                .build()
                .setStreamName(mapA)
                .setStreamTags(CorfuRuntime.getStreamID(streamA))
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .open();

        // TODO: fix so this does not require mapCopy.
        // Since we only allow one MVO for an object per runtime,
        // NO_CACHE must not be used here. Instead, use another runtime.
        ICorfuTable<String, String> mapCopy = getNewRuntime(getDefaultNode()).connect().getObjectsView()
                .build()
                .setStreamName(mapA)
                .setStreamTags(CorfuRuntime.getStreamID(streamA))
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .open();

        map.insert("initial", "value");

        Semaphore s1 = new Semaphore(0);
        Semaphore s2 = new Semaphore(0);

        // Schedule two threads, the first starts a transaction and reads,
        // then waits for the second thread to finish.
        // the second starts a transaction, waits for the first tx to read
        // and commits.
        // The first thread then resumes and attempts to commit. It should abort.
        scheduleConcurrently(1, t -> {
            assertThatThrownBy(() -> {
                getRuntime().getObjectsView().TXBegin();
                map.get("k");
                s1.release();   // Let thread 2 start.
                s2.acquire();   // Wait for thread 2 to commit.
                map.insert("k", "v1");
                getRuntime().getObjectsView().TXEnd();
            }).isInstanceOf(TransactionAbortedException.class);
        });

        scheduleConcurrently(1, t -> {
            s1.acquire();   // Wait for thread 1 to read
            getRuntime().getObjectsView().TXBegin();
            mapCopy.insert("k", "v2");
            getRuntime().getObjectsView().TXEnd();
            s2.release();
        });

        executeScheduled(2, PARAMETERS.TIMEOUT_LONG);

        // The result should contain T2s modification.
        assertThat(map.get("k")).isEqualTo("v2");

        IStreamView taggedStream =
            r.getStreamsView().get(CorfuRuntime.getStreamID(streamA));
        List<ILogData> updates = taggedStream.remainingUpTo(Long.MAX_VALUE);
        assertThat(updates).hasSize(1);
        assertThat(updates.get(0).getLogEntry(getRuntime()).getType())
                .isEqualTo(LogEntry.LogEntryType.MULTIOBJSMR);

        MultiObjectSMREntry tx1 =
            (MultiObjectSMREntry)updates.get(0).getLogEntry
                (getRuntime());
        MultiSMREntry entryMap = tx1.getEntryMap().get(CorfuRuntime.getStreamID(mapA));
        assertThat(entryMap).isNotNull();

        assertThat(entryMap.getUpdates().size()).isEqualTo(1);

        SMREntry smrEntry = entryMap.getUpdates().get(0);
        Object[] args = smrEntry.getSMRArguments();
        assertThat(smrEntry.getSMRMethod()).isEqualTo("put");
        assertThat((String) args[0]).isEqualTo("k");
        assertThat((String) args[1]).isEqualTo("v2");
    }

    @Test
    public void incorrectNestingTest() {
        CorfuRuntime r1 = getDefaultRuntime();
        CorfuRuntime r2 = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder()
                .nettyEventLoop(NETTY_EVENT_LOOP)
                .build());
        ICorfuTable<String, String> map = getDefaultRuntime().getObjectsView()
                .build()
                .setStreamName("mapa")
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .open();

        r1.getObjectsView().TXBegin();
        map.get("key1");

        // Try to start a new nested transaction with a different runtime
        assertThatThrownBy(() -> r2.getObjectsView().TXBegin()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void unrelatedStreamDoesNotConflict()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime();

        ICorfuTable<String, String> smrMap = r.getObjectsView().build()
                .setStreamName("map a")
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .open();

        IStreamView streamB = r.getStreamsView().get(CorfuRuntime.getStreamID("b"));
        smrMap.insert("a", "b");
        streamB.append(new SMREntry("hi", new Object[]{"hello"}, Serializers.PRIMITIVE));

        //this TX should not conflict
        assertThat(smrMap.containsKey("b")).isFalse();

        r.getObjectsView().TXBegin();
        String b = smrMap.get("a");
        smrMap.insert("b", b);
        r.getObjectsView().TXEnd();

        assertThat(smrMap.get("b")).isEqualTo("b");
    }

    @Test
    public void unrelatedTransactionDoesNotConflict()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime();

        ICorfuTable<String, String> smrMap = r.getObjectsView().build()
                .setStreamName("map a")
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .open();

        ICorfuTable<String, String> smrMapB = r.getObjectsView().build()
                .setStreamName("map b")
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .open();

        smrMap.insert("a", "b");

        r.getObjectsView().TXBegin();
        String b = smrMap.get("a");
        smrMapB.insert("b", b);
        r.getObjectsView().TXEnd();

        //this TX should not conflict
        assertThat(smrMap.containsKey("b")).isFalse();
        r.getObjectsView().TXBegin();
        b = smrMap.get("a");
        smrMap.insert("b", b);
        r.getObjectsView().TXEnd();

        assertThat(smrMap.get("b")).isEqualTo("b");
    }

}

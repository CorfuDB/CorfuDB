package org.corfudb.runtime.view;

import com.google.common.reflect.TypeToken;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 2/18/16.
 */
public class ObjectsViewTest extends AbstractViewTest {

    public static boolean referenceTX(Map<String, String> smrMap) {
        smrMap.put("a", "b");
        assertThat(smrMap)
                .containsEntry("a", "b");
        return true;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canAbortNoTransaction()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime();
        r.getObjectsView().TXAbort();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void abortedTransactionDoesNotConflict()
            throws Exception {
        final String mapA = "map a";
        //Enbale transaction logging
        CorfuRuntime r = getDefaultRuntime()
                .setTransactionLogging(true);

        SMRMap<String, String> map = getDefaultRuntime().getObjectsView()
                .build()
                .setStreamName(mapA)
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        // TODO: fix so this does not require mapCopy.
        SMRMap<String, String> mapCopy = getDefaultRuntime().getObjectsView()
                .build()
                .setStreamName(mapA)
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .addOption(ObjectOpenOptions.NO_CACHE)
                .open();


        map.put("initial", "value");

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
                map.put("k", "v1");
                getRuntime().getObjectsView().TXEnd();
            }).isInstanceOf(TransactionAbortedException.class);
        });

        scheduleConcurrently(1, t -> {
            s1.acquire();   // Wait for thread 1 to read
            getRuntime().getObjectsView().TXBegin();
            mapCopy.put("k", "v2");
            getRuntime().getObjectsView().TXEnd();
            s2.release();
        });

        executeScheduled(2, PARAMETERS.TIMEOUT_LONG);

        // The result should contain T2s modification.
        assertThat(map)
                .containsEntry("k", "v2");

        IStreamView txStream = r.getStreamsView().get(ObjectsView
                .TRANSACTION_STREAM_ID);
        List<ILogData> txns = txStream.remainingUpTo(Long.MAX_VALUE);
        assertThat(txns).hasSize(1);
        assertThat(txns.get(0).getLogEntry(getRuntime()).getType())
                .isEqualTo(LogEntry.LogEntryType.MULTIOBJSMR);

        MultiObjectSMREntry tx1 = (MultiObjectSMREntry)txns.get(0).getLogEntry
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
    @SuppressWarnings("unchecked")
    public void unrelatedStreamDoesNotConflict()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime();

        Map<String, String> smrMap = r.getObjectsView().build()
                .setStreamName("map a")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        IStreamView streamB = r.getStreamsView().get(CorfuRuntime.getStreamID("b"));
        smrMap.put("a", "b");
        streamB.append(new SMREntry("hi", new Object[]{"hello"}, Serializers.PRIMITIVE));

        //this TX should not conflict
        assertThat(smrMap)
                .doesNotContainKey("b");
        r.getObjectsView().TXBegin();
        String b = smrMap.get("a");
        smrMap.put("b", b);
        r.getObjectsView().TXEnd();

        assertThat(smrMap)
                .containsEntry("b", "b");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void unrelatedTransactionDoesNotConflict()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime();

        Map<String, String> smrMap = r.getObjectsView().build()
                .setStreamName("map a")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        Map<String, String> smrMapB = r.getObjectsView().build()
                .setStreamName("map b")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        smrMap.put("a", "b");

        r.getObjectsView().TXBegin();
        String b = smrMap.get("a");
        smrMapB.put("b", b);
        r.getObjectsView().TXEnd();

        //this TX should not conflict
        assertThat(smrMap)
                .doesNotContainKey("b");
        r.getObjectsView().TXBegin();
        b = smrMap.get("a");
        smrMap.put("b", b);
        r.getObjectsView().TXEnd();

        assertThat(smrMap)
                .containsEntry("b", "b");
    }

}

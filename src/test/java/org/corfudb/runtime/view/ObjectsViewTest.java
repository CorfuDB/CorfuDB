package org.corfudb.runtime.view;

import lombok.Getter;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.logprotocol.TXEntry;
import org.corfudb.protocols.wireprotocol.ILogUnitEntry;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

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
    public void canCopyObject()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime();

        Map<String, String> smrMap = r.getObjectsView().open("map a", SMRMap.class);
        smrMap.put("a", "a");
        Map<String, String> smrMapCopy = r.getObjectsView().copy(smrMap, "map a copy");
        smrMapCopy.put("b", "b");

        assertThat(smrMapCopy)
                .containsEntry("a", "a")
                .containsEntry("b", "b");

        assertThat(smrMap)
                .containsEntry("a", "a")
                .doesNotContainEntry("b", "b");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void cannotCopyNonCorfuObject()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime();

        assertThatThrownBy(() -> {
            r.getObjectsView().copy(new HashMap<String, String>(), CorfuRuntime.getStreamID("test"));
        }).isInstanceOf(RuntimeException.class);
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
        //Enbale transaction logging
        CorfuRuntime r = getDefaultRuntime()
                .setTransactionLogging(true);

        Map<String, String> smrMap = r.getObjectsView().open("map a", SMRMap.class);
        smrMap.put("a", "b");

        //generate an aborted TX
        r.getObjectsView().TXBegin();
        String b = smrMap.get("a");
        smrMap.put("b", b);
        StreamView sv = r.getStreamsView().get(CorfuRuntime.getStreamID("map a"));
        LogData rr = sv.read();
        sv.write(rr.getPayload(getRuntime()));
        assertThatThrownBy(() -> {
            r.getObjectsView().TXEnd();
        }).isInstanceOf(TransactionAbortedException.class);

        //this TX should not conflict
        assertThat(smrMap)
                .doesNotContainKey("b");
        r.getObjectsView().TXBegin();
        b = smrMap.get("a");
        smrMap.put("b", b);
        r.getObjectsView().TXEnd();

        assertThat(smrMap)
                .containsEntry("b", "b");

        // The transaction stream should have two transaction entries, one for the first
        // failed transaction and the other for successful transaction
        StreamView txStream = r.getStreamsView().get(ObjectsView.TRANSACTION_STREAM_ID);
        LogData[] txns = txStream.readTo(Long.MAX_VALUE);
        assertThat(txns.length).isEqualTo(2);
        assertThat(txns[0].getLogEntry(getRuntime()).getType()).isEqualTo(LogEntry.LogEntryType.TX);
        assertThat(txns[1].getLogEntry(getRuntime()).getType()).isEqualTo(LogEntry.LogEntryType.TX);
        TXEntry tx1 = (TXEntry)txns[0].getLogEntry(getRuntime());
        TXEntry tx2 = (TXEntry)txns[1].getLogEntry(getRuntime());
        assertThat(tx1.isAborted()).isEqualTo(true);
        assertThat(tx2.isAborted()).isEqualTo(false);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void unrelatedStreamDoesNotConflict()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime();

        Map<String, String> smrMap = r.getObjectsView().open("map a", SMRMap.class);
        StreamView streamB = r.getStreamsView().get(CorfuRuntime.getStreamID("b"));
        smrMap.put("a", "b");
        streamB.write(new SMREntry("hi", new Object[]{"hello"}, Serializers.SerializerType.PRIMITIVE));

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

        Map<String, String> smrMap = r.getObjectsView().open("map a", SMRMap.class);
        Map<String, String> smrMapB = r.getObjectsView().open("map b", SMRMap.class);

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

    @Test
    @SuppressWarnings("unchecked")
    public void canRunLambdaTransaction()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime();
        Map<String, String> smrMap = r.getObjectsView().open("map a", SMRMap.class);

        assertThat(r.getObjectsView().executeTX(() -> {
            smrMap.put("a", "b");
            assertThat(smrMap)
                    .containsEntry("a", "b");
            return true;
        })).isTrue();

        assertThat(smrMap)
                .containsEntry("a", "b");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canRunLambdaReferenceTransaction()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime();
        Map<String, String> smrMap = r.getObjectsView().open("map a", SMRMap.class);

        assertThat(r.getObjectsView().executeTX(ObjectsViewTest::referenceTX, smrMap))
                .isEqualTo(true);

        assertThat(smrMap)
                .containsEntry("a", "b");
    }
}

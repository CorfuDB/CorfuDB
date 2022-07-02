package org.corfudb.infrastructure;

import com.google.protobuf.Message;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuCompactorManagement.ActiveCPStreamMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.SequencerView;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.function.Predicate;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doNothing;

public class LivenessValidatorUnitTest {

    private final TxnContext txn = mock(TxnContext.class);
    private final CorfuStoreEntry corfuStoreEntry = mock(CorfuStoreEntry.class);
    private final StreamAddressSpace streamAddressSpace = mock(StreamAddressSpace.class);

    private final static String NAMESPACE = "TestNamespace";
    private final static String TABLE_NAME = "TestTableName";
    private final static int TIMEOUT_SECONDS = 3;
    private final static TableName tableName = TableName.newBuilder().setNamespace(NAMESPACE).setTableName(TABLE_NAME).build();

    private LivenessValidator livenessValidator;

    @Before
    public void setup() {
        CorfuRuntime corfuRuntime = mock(CorfuRuntime.class);
        CorfuStore corfuStore = mock(CorfuStore.class);
        SequencerView sequencerView = mock(SequencerView.class);

        this.livenessValidator = new LivenessValidator(corfuRuntime, corfuStore, Duration.ofSeconds(TIMEOUT_SECONDS));

        when(corfuRuntime.getSequencerView()).thenReturn(sequencerView);
        when(sequencerView.getStreamAddressSpace(any(StreamAddressRange.class))).thenReturn(streamAddressSpace);
        when(corfuStore.txn(CORFU_SYSTEM_NAMESPACE)).thenReturn(txn);
        when(txn.getRecord(anyString(), any(Message.class))).thenReturn(corfuStoreEntry);
        doNothing().when(txn).putRecord(any(), any(), any(), any());
        doNothing().when(txn).delete(anyString(), any(Message.class));
        when(txn.commit()).thenReturn(Timestamp.getDefaultInstance());
    }

    @Test
    public void isTableCheckpointActiveTest() {
        when(corfuStoreEntry.getPayload()).thenReturn(ActiveCPStreamMsg.newBuilder().setSyncHeartbeat(-1).build());
        assert livenessValidator.isTableCheckpointActive(tableName, Duration.ofSeconds(0));
        assert livenessValidator.isTableCheckpointActive(tableName, Duration.ofSeconds(TIMEOUT_SECONDS));
        //ActiveCheckpointsTable with same heartbeat value for more than timeout time
        assert !livenessValidator.isTableCheckpointActive(tableName, Duration.ofSeconds(TIMEOUT_SECONDS + 1));
        livenessValidator.clearLivenessMap();

        //assert that isTableCheckpointActive returns true if one of the fields(heartbeat, streamTail) increases over time
        when(streamAddressSpace.getTail()).thenReturn(1L);
        assert livenessValidator.isTableCheckpointActive(tableName, Duration.ofSeconds(0));
        when(corfuStoreEntry.getPayload()).thenReturn(ActiveCPStreamMsg.newBuilder().setSyncHeartbeat(1).build());
        assert livenessValidator.isTableCheckpointActive(tableName, Duration.ofSeconds(TIMEOUT_SECONDS + 1));
        when(streamAddressSpace.getTail()).thenReturn(2L);
        assert livenessValidator.isTableCheckpointActive(tableName, Duration.ofSeconds(TIMEOUT_SECONDS * 2 + 1));

        when(corfuStoreEntry.getPayload()).thenReturn(null);
        assert livenessValidator.isTableCheckpointActive(tableName, Duration.ofSeconds(0));
    }

    @Test
    public void shouldChangeManagerStatusTest() {
        List<CorfuStoreEntry> mockList = mock(List.class);
        when(txn.executeQuery(anyString(), any(Predicate.class))).thenReturn(mockList);

        when(mockList.size()).thenReturn(1);
        Assert.assertEquals(LivenessValidator.StatusToChange.NONE,
                livenessValidator.shouldChangeManagerStatus(Duration.ofSeconds(0)));

        when(corfuStoreEntry.getPayload()).thenReturn(CheckpointingStatus.newBuilder()
                .setStatus(StatusType.STARTED).build());
        Assert.assertEquals(LivenessValidator.StatusToChange.NONE,
                livenessValidator.shouldChangeManagerStatus(Duration.ofSeconds(TIMEOUT_SECONDS))); //TODO
        Assert.assertEquals(LivenessValidator.StatusToChange.FINISH,
                livenessValidator.shouldChangeManagerStatus(Duration.ofSeconds(TIMEOUT_SECONDS + 1)));
        livenessValidator.clearLivenessValidator();
        livenessValidator.clearLivenessMap();

        when(mockList.size()).thenReturn(0);
        Assert.assertEquals(LivenessValidator.StatusToChange.NONE,
                livenessValidator.shouldChangeManagerStatus(Duration.ofSeconds(0)));
        Assert.assertEquals(LivenessValidator.StatusToChange.FINISH,
                livenessValidator.shouldChangeManagerStatus(Duration.ofSeconds(TIMEOUT_SECONDS + 1)));
    }
}

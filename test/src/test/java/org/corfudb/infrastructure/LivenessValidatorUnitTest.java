package org.corfudb.infrastructure;

import com.google.protobuf.Message;
import org.corfudb.runtime.CorfuCompactorManagement.ActiveCPStreamMsg;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.SequencerView;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.function.Predicate;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class LivenessValidatorUnitTest {

    private final TxnContext txn = mock(TxnContext.class);
    private final CorfuStoreEntry corfuStoreEntry = mock(CorfuStoreEntry.class);

    private static final String NAMESPACE = "TestNamespace";
    private static final String TABLE_NAME = "TestTableName";
    private static final int TIMEOUT_SECONDS = 3;
    private static final TableName tableName = TableName.newBuilder().setNamespace(NAMESPACE).setTableName(TABLE_NAME).build();

    private LivenessValidator livenessValidatorSpy;

    @Before
    public void setup() {
        CorfuRuntime corfuRuntime = mock(CorfuRuntime.class);
        CorfuStore corfuStore = mock(CorfuStore.class);
        SequencerView sequencerView = mock(SequencerView.class);

        LivenessValidator livenessValidator = new LivenessValidator(corfuRuntime, corfuStore, Duration.ofSeconds(TIMEOUT_SECONDS));
        this.livenessValidatorSpy = spy(livenessValidator);

        when(corfuRuntime.getSequencerView()).thenReturn(sequencerView);
        when(corfuStore.txn(CORFU_SYSTEM_NAMESPACE)).thenReturn(txn);
        when(txn.getRecord(anyString(), any(Message.class))).thenReturn(corfuStoreEntry);
        doNothing().when(txn).putRecord(any(), any(), any(), any());
        doNothing().when(txn).delete(anyString(), any(Message.class));
        when(txn.commit()).thenReturn(Timestamp.getDefaultInstance());
    }

    @Test
    public void isTableCheckpointActiveTest() {
        when(corfuStoreEntry.getPayload()).thenReturn(ActiveCPStreamMsg.newBuilder().setSyncHeartbeat(-1).build());
        doReturn(1L).when(livenessValidatorSpy).getCpStreamTail(any());
        assert livenessValidatorSpy.isTableCheckpointActive(tableName, Duration.ofSeconds(0));
        assert livenessValidatorSpy.isTableCheckpointActive(tableName, Duration.ofSeconds(TIMEOUT_SECONDS));
        //ActiveCheckpointsTable with same heartbeat value for more than timeout time
        assert !livenessValidatorSpy.isTableCheckpointActive(tableName, Duration.ofSeconds(TIMEOUT_SECONDS + 1));
        livenessValidatorSpy.clearLivenessMap();

        //assert that isTableCheckpointActive returns true if one of the fields(heartbeat, streamTail) increases over time
        doReturn(1L).when(livenessValidatorSpy).getCpStreamTail(any());
        assert livenessValidatorSpy.isTableCheckpointActive(tableName, Duration.ofSeconds(0));
        when(corfuStoreEntry.getPayload()).thenReturn(ActiveCPStreamMsg.newBuilder().setSyncHeartbeat(1).build());
        assert livenessValidatorSpy.isTableCheckpointActive(tableName, Duration.ofSeconds(TIMEOUT_SECONDS + 1));
        doReturn(2L).when(livenessValidatorSpy).getCpStreamTail(any());
        assert livenessValidatorSpy.isTableCheckpointActive(tableName, Duration.ofSeconds(TIMEOUT_SECONDS * 2 + 1));

        when(corfuStoreEntry.getPayload()).thenReturn(null);
        assert livenessValidatorSpy.isTableCheckpointActive(tableName, Duration.ofSeconds(0));
    }

    @Test
    public void shouldChangeManagerStatusTest() {
        List<CorfuStoreEntry<? extends Message, ? extends Message, ? extends Message>> mockList = mock(List.class);
        doReturn(1).when(livenessValidatorSpy).getIdleCount();

        Assert.assertEquals(LivenessValidator.Status.NONE,
                livenessValidatorSpy.shouldChangeManagerStatus(Duration.ofSeconds(0)));

        when(corfuStoreEntry.getPayload()).thenReturn(CheckpointingStatus.newBuilder()
                .setStatus(StatusType.STARTED).build());
        Assert.assertEquals(LivenessValidator.Status.NONE,
                livenessValidatorSpy.shouldChangeManagerStatus(Duration.ofSeconds(TIMEOUT_SECONDS)));
        Assert.assertEquals(LivenessValidator.Status.FINISH,
                livenessValidatorSpy.shouldChangeManagerStatus(Duration.ofSeconds(TIMEOUT_SECONDS + 1)));
        livenessValidatorSpy.clearLivenessValidator();
        livenessValidatorSpy.clearLivenessMap();

        when(mockList.size()).thenReturn(0);
        Assert.assertEquals(LivenessValidator.Status.NONE,
                livenessValidatorSpy.shouldChangeManagerStatus(Duration.ofSeconds(0)));
        Assert.assertEquals(LivenessValidator.Status.FINISH,
                livenessValidatorSpy.shouldChangeManagerStatus(Duration.ofSeconds(TIMEOUT_SECONDS + 1)));

        final int currentTime = 4;
        when(corfuStoreEntry.getPayload()).thenThrow(new RuntimeException());
        when(mockList.size()).thenReturn(1);
        Assert.assertEquals(LivenessValidator.Status.NONE,
                livenessValidatorSpy.shouldChangeManagerStatus(Duration.ofSeconds(currentTime)));
    }
}

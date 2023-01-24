package org.corfudb.compactor;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.DistributedCheckpointer;
import org.corfudb.runtime.DistributedCheckpointerHelper;
import org.corfudb.runtime.collections.CorfuStore;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CorfuStoreCompactorMainUnitTest {
    private final CorfuRuntime corfuRuntime = mock(CorfuRuntime.class);
    private final CorfuRuntime cpRuntime = mock(CorfuRuntime.class);
    private final CorfuStore corfuStore = mock(CorfuStore.class);
    private final CorfuStoreCompactorConfig config = mock(CorfuStoreCompactorConfig.class);
    private final DistributedCheckpointerHelper distributedCheckpointerHelper = mock(DistributedCheckpointerHelper.class);
    private final DistributedCheckpointer distributedCheckpointer = mock(DistributedCheckpointer.class);
    private CorfuStoreCompactorMain corfuStoreCompactorMain;

    @Before
    public void setup() {
        when(corfuRuntime.getParameters()).thenReturn(CorfuRuntime.CorfuRuntimeParameters.builder().clientName("TestClient").build());
        corfuStoreCompactorMain = spy(new CorfuStoreCompactorMain(config, corfuRuntime, cpRuntime, corfuStore, distributedCheckpointerHelper));
        doReturn(true).when(distributedCheckpointerHelper).disableCompaction();
        doNothing().when(distributedCheckpointerHelper).enableCompaction();
        doNothing().when(distributedCheckpointerHelper).freezeCompaction();
        doNothing().when(distributedCheckpointerHelper).unfreezeCompaction();
        doNothing().when(distributedCheckpointerHelper).instantTrigger(anyBoolean());
        doNothing().when(distributedCheckpointer).checkpointTables();
    }

    @Test
    public void doCompactorActionHappyPath() {
        doReturn(true).when(config).isFreezeCompaction();
        doReturn(true).when(config).isDisableCompaction();
        corfuStoreCompactorMain.doCompactorAction();

        doReturn(false).when(config).isFreezeCompaction();
        doReturn(false).when(config).isDisableCompaction();
        doReturn(true).when(config).isUnfreezeCompaction();
        doReturn(true).when(config).isEnableCompaction();
        doReturn(true).when(config).isInstantTriggerCompaction();
        doReturn(true).when(config).isStartCheckpointing();
        doNothing().when(corfuStoreCompactorMain).checkpoint(any());
        when(config.isTrim()).thenReturn(true).thenReturn(false);
        corfuStoreCompactorMain.doCompactorAction();
        corfuStoreCompactorMain.doCompactorAction();

        verify(distributedCheckpointerHelper).disableCompaction();
        verify(distributedCheckpointerHelper).freezeCompaction();
        verify(distributedCheckpointerHelper, times(2)).enableCompaction();
        verify(distributedCheckpointerHelper, times(2)).unfreezeCompaction();
        verify(corfuStoreCompactorMain, times(2)).checkpoint(any());
        verify(distributedCheckpointerHelper).instantTrigger(true);
        verify(distributedCheckpointerHelper).instantTrigger(false);
    }

    @Test
    public void doCompactorActionIncorrectConfigCombinations() {
        doReturn(true).when(config).isFreezeCompaction();
        doReturn(true).when(config).isUnfreezeCompaction();
        corfuStoreCompactorMain.doCompactorAction();

        doReturn(true).when(config).isDisableCompaction();
        doReturn(true).when(config).isEnableCompaction();
        doReturn(false).when(config).isFreezeCompaction();
        doReturn(false).when(config).isUnfreezeCompaction();
        corfuStoreCompactorMain.doCompactorAction();

        verify(distributedCheckpointerHelper).freezeCompaction();
        verify(distributedCheckpointerHelper).disableCompaction();
        verify(distributedCheckpointerHelper, never()).unfreezeCompaction();
        verify(distributedCheckpointerHelper, never()).enableCompaction();
    }

    @Test
    public void checkpointFailureTest() {
        doReturn(false).when(distributedCheckpointerHelper).hasCompactionStarted();
        corfuStoreCompactorMain.checkpoint(distributedCheckpointer);

        verify(distributedCheckpointerHelper, times(CorfuStoreCompactorMain.RETRY_CHECKPOINTING)).hasCompactionStarted();
        verify(distributedCheckpointer).shutdown();
    }

    @Test
    public void checkpointSuccessTest() {
        when(distributedCheckpointerHelper.hasCompactionStarted()).thenReturn(false).thenReturn(true);
        corfuStoreCompactorMain.checkpoint(distributedCheckpointer);

        verify(distributedCheckpointerHelper, times(2)).hasCompactionStarted();
        verify(distributedCheckpointer).shutdown();
    }
}

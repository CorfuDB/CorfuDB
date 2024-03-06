package org.corfudb.compactor;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.DistributedCheckpointer;
import org.corfudb.runtime.DistributedCheckpointerHelper;
import org.corfudb.runtime.collections.CorfuStore;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

    public class CompactorClientUnitTest {
    private final CorfuRuntime corfuRuntime = mock(CorfuRuntime.class);
    private final CorfuRuntime cpRuntime = mock(CorfuRuntime.class);
    private final CorfuStore corfuStore = mock(CorfuStore.class);
    private final CompactorControllerConfig config = mock(CompactorControllerConfig.class);
    private final DistributedCheckpointerHelper distributedCheckpointerHelper = mock(DistributedCheckpointerHelper.class);
    private final DistributedCheckpointer distributedCheckpointer = mock(DistributedCheckpointer.class);
    private CompactorController corfuCompactorControls;
    private CompactorCheckpointer corfuCompactorCheckpointer;

    @Before
    public void setup() {
        when(corfuRuntime.getParameters()).thenReturn(CorfuRuntime.CorfuRuntimeParameters.builder().clientName("TestClient").build());
        corfuCompactorControls = spy(new CompactorController(config, corfuRuntime, corfuStore, distributedCheckpointerHelper));
        corfuCompactorCheckpointer = spy(new CompactorCheckpointer(cpRuntime, corfuRuntime, distributedCheckpointerHelper, distributedCheckpointer));
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
        corfuCompactorControls.doCompactorAction();

        doReturn(false).when(config).isFreezeCompaction();
        doReturn(false).when(config).isDisableCompaction();
        doReturn(true).when(config).isUnfreezeCompaction();
        doReturn(true).when(config).isEnableCompaction();
        doReturn(true).when(config).isInstantTriggerCompaction();
        when(config.isTrim()).thenReturn(true).thenReturn(false);
        corfuCompactorControls.doCompactorAction();
        corfuCompactorControls.doCompactorAction();

        corfuCompactorControls.shutdown();

        verify(distributedCheckpointerHelper).disableCompaction();
        verify(distributedCheckpointerHelper).freezeCompaction();
        verify(distributedCheckpointerHelper, times(2)).enableCompaction();
        verify(distributedCheckpointerHelper, times(2)).unfreezeCompaction();
        verify(distributedCheckpointerHelper).instantTrigger(true);
        verify(distributedCheckpointerHelper).instantTrigger(false);
        verify(corfuCompactorControls.getCorfuRuntime()).shutdown();
    }

    @Test
    public void doCompactorActionIncorrectConfigCombinations() {
        doReturn(true).when(config).isFreezeCompaction();
        doReturn(true).when(config).isUnfreezeCompaction();
        corfuCompactorControls.doCompactorAction();

        doReturn(true).when(config).isDisableCompaction();
        doReturn(true).when(config).isEnableCompaction();
        doReturn(false).when(config).isFreezeCompaction();
        doReturn(false).when(config).isUnfreezeCompaction();
        corfuCompactorControls.doCompactorAction();

        corfuCompactorControls.shutdown();

        verify(distributedCheckpointerHelper).freezeCompaction();
        verify(distributedCheckpointerHelper).disableCompaction();
        verify(distributedCheckpointerHelper, never()).unfreezeCompaction();
        verify(distributedCheckpointerHelper, never()).enableCompaction();
        verify(corfuCompactorControls.getCorfuRuntime()).shutdown();
    }

    @Test
    public void unableToCheckpointTest() {
        doReturn(false).when(distributedCheckpointerHelper).hasCompactionStarted();
        corfuCompactorCheckpointer.startCheckpointing();
        corfuCompactorCheckpointer.shutdown();

        verify(distributedCheckpointerHelper, times(CompactorCheckpointer.RETRY_CHECKPOINTING)).hasCompactionStarted();
        verify(corfuRuntime).shutdown();
        verify(cpRuntime).shutdown();
        verify(distributedCheckpointer).shutdown();
    }

    @Test
    public void checkpointSuccessTest() {
        when(distributedCheckpointerHelper.hasCompactionStarted()).thenReturn(false).thenReturn(true);
        corfuCompactorCheckpointer.startCheckpointing();
        corfuCompactorCheckpointer.shutdown();

        verify(distributedCheckpointerHelper, times(2)).hasCompactionStarted();
        verify(corfuRuntime).shutdown();
        verify(cpRuntime).shutdown();
        verify(distributedCheckpointer).shutdown();
    }
}

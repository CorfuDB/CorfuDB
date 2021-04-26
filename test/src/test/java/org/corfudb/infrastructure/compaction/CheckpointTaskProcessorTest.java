package org.corfudb.infrastructure.compaction;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class CheckpointTaskProcessorTest extends AbstractViewTest {

    private CorfuRuntime corfuRuntime;
    private CheckpointOptions options;
    private CheckpointTaskRequest taskRequest;
    private final String diskModePath = "/tmp";

    private static final String NAMESPACE = "TEST_NAMESPACE";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String TASK_ID = "TASK_ID";
    private static final Instant START_TIME = Instant.now();

    @Before
    public void setup() {
        corfuRuntime = getDefaultRuntime();
        SafeSnapshot safeSnapshot = SafeSnapshot.builder()
                .address(1L)
                .committed(true)
                .recordTime(START_TIME)
                .build();
        options = CheckpointOptions.builder()
                .isDiskBacked(false)
                .build();
        taskRequest = CheckpointTaskRequest.builder()
                .namespace(NAMESPACE)
                .tableName(TABLE_NAME)
                .compactionTaskId(TASK_ID)
                .safeSnapshot(safeSnapshot)
                .options(options)
                .build();
    }

    /**
     * Tests the task response has correct contents when task is finished.
     */
    @Test
    public void testTaskResponseWhenFinished() {
        CheckpointTaskProcessor taskProcessor = new CheckpointTaskProcessor(diskModePath, corfuRuntime);
        CheckpointTaskResponse taskResponse = taskProcessor.executeCheckpointTask(taskRequest);
        assertThat(taskResponse.getRequest()).isEqualTo(taskRequest);
        assertThat(taskResponse.getStatus()).isEqualTo(CheckpointTaskResponse.Status.FINISHED);
        assertThat(taskResponse.getCauseOfFailure()).isEmpty();
    }

    /**
     * Tests the task response has correct contents when task is disk based.
     */
    @Test
    public void testTaskWithDiskBacked() {
        // make task request be disk-backed
        CheckpointOptions diskBacked = options.toBuilder().isDiskBacked(true).build();
        taskRequest = taskRequest.toBuilder().options(diskBacked).build();
        CheckpointTaskProcessor taskProcessor = new CheckpointTaskProcessor(diskModePath, corfuRuntime);
        CheckpointTaskResponse taskResponse = taskProcessor.executeCheckpointTask(taskRequest);
        assertThat(taskResponse.getRequest()).isEqualTo(taskRequest);
        assertThat(taskResponse.getStatus()).isEqualTo(CheckpointTaskResponse.Status.FINISHED);
        assertThat(taskResponse.getCauseOfFailure()).isEmpty();
    }

    /**
     * Tests the task response has correct contents when task is failed.
     */
    @Test
    public void testTaskResponseWhenFailed() {
        // make task request be disk-backed, and do not pass disk mode path.
        // open table will fail
        CheckpointOptions diskBacked = options.toBuilder().isDiskBacked(true).build();
        taskRequest = taskRequest.toBuilder().options(diskBacked).build();
        CheckpointTaskProcessor taskProcessor = new CheckpointTaskProcessor(null, corfuRuntime);
        CheckpointTaskResponse taskResponse = taskProcessor.executeCheckpointTask(taskRequest);
        assertThat(taskResponse.getRequest()).isEqualTo(taskRequest);
        assertThat(taskResponse.getStatus()).isEqualTo(CheckpointTaskResponse.Status.FAILED);
        assertThat(taskResponse.getCauseOfFailure()).isNotEmpty();
        assertThat(taskResponse.getCauseOfFailure().get()).hasCauseInstanceOf(IllegalArgumentException.class);
    }
}

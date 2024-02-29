package org.corfudb.infrastructure.log;

import org.corfudb.infrastructure.BatchProcessor.BatchProcessorContext;
import org.corfudb.infrastructure.log.FileSystemAgent.FileSystemConfig;
import org.corfudb.infrastructure.log.StreamLog.PersistenceMode;
import org.corfudb.runtime.proto.FileSystemStats.BatchProcessorStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

public class FileSystemAgentTest {

    @TempDir
    File logDir;

    @Test
    public void testFileSystemAgentUpdateBatchProcessorStatus() throws Exception {

        // Init the FileSystemAgent
        FileSystemConfig config = new FileSystemConfig(logDir.toPath(),100, 0, PersistenceMode.DISK, Duration.ofMillis(50));
        BatchProcessorContext batchProcessorContext = new BatchProcessorContext();
        FileSystemAgent.init(config, batchProcessorContext);

        // Check  PartitionAttribute's BatchProcessor status is initialized to OK
        assertThat(FileSystemAgent.getPartitionAttribute().getBatchProcessorStatus()
                .equals(BatchProcessorStatus.BP_STATUS_OK)).isTrue();

        // Set Batch Processor Error Status
        batchProcessorContext.setErrorStatus();


        boolean result = false;
        for (int i = 0; i < 10; i++) {
            BatchProcessorStatus batchProcessorStatus = FileSystemAgent
                    .getPartitionAttribute()
                    .getBatchProcessorStatus();
            if (batchProcessorStatus == BatchProcessorStatus.BP_STATUS_ERROR) {
                result = true;
                break;
            }
            Thread.sleep(50);
        }

        // Check that the updated value is reflected
        assertThat(result).isTrue();
    }
}

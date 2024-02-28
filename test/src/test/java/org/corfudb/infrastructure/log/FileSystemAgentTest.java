package org.corfudb.infrastructure.log;

import org.corfudb.infrastructure.BatchProcessor.BatchProcessorContext;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.log.FileSystemAgent.FileSystemConfig;
import org.corfudb.runtime.proto.FileSystemStats.BatchProcessorStatus;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FileSystemAgentTest {

    @Test
    public void testFileSystemAgentUpdateBatchProcessorStatus() throws Exception {
        // Init the FileSystemAgent
        String logDir = com.google.common.io.Files.createTempDir().getAbsolutePath();
        ServerContext context = new ServerContextBuilder()
                .setLogPath(logDir)
                .setMemory(false)
                .build();
        FileSystemConfig config = new FileSystemConfig(context);
        BatchProcessorContext batchProcessorContext = new BatchProcessorContext();
        FileSystemAgent.init(config, batchProcessorContext);

        // Check  PartitionAttribute's BatchProcessor status is initialized to OK
        assertThat(FileSystemAgent.getPartitionAttribute().getBatchProcessorStatus()
                .equals(BatchProcessorStatus.BP_STATUS_OK)).isTrue();

        // Set Batch Processor Error Status
        batchProcessorContext.setErrorStatus();

        // Allow time for PartitionAttribute's scheduled updater thread to run
        Thread.sleep(10000);

        // Check that the updated value is reflected
        assertThat(FileSystemAgent.getPartitionAttribute().getBatchProcessorStatus()
                .equals(BatchProcessorStatus.BP_STATUS_ERROR)).isTrue();
    }
}

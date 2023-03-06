package org.corfudb.protocols.wireprotocol.failuredetector;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.runtime.proto.FileSystemStats.BatchProcessorStatus;

@AllArgsConstructor
@EqualsAndHashCode
@Getter
@ToString
public class FileSystemStats {

    @NonNull
    private final PartitionAttributeStats partitionAttributeStats;
    @NonNull
    private final BatchProcessorStats batchProcessorStats;

    public boolean hasError() {
        boolean isReadOnly = partitionAttributeStats.isReadOnly();
        boolean isBpError = batchProcessorStats.isError();

        return isReadOnly || isBpError;
    }

    public boolean isOk() {
        return !hasError();
    }

    @AllArgsConstructor
    @Getter
    @EqualsAndHashCode
    @ToString
    public static class PartitionAttributeStats {
        private final boolean readOnly;
        private final long availableSpace;
        private final long totalSpace;

        public boolean isWritable() {
            return !readOnly;
        }
    }

    @AllArgsConstructor
    @Getter
    @EqualsAndHashCode
    @ToString
    public static class BatchProcessorStats {
        @Getter
        private final BatchProcessorStatus status;

        public static final BatchProcessorStats OK = new BatchProcessorStats(BatchProcessorStatus.OK);
        public static final BatchProcessorStats ERR = new BatchProcessorStats(BatchProcessorStatus.ERROR);

        public boolean isError() {
            return status == BatchProcessorStatus.ERROR;
        }

        public boolean isOk() {
            return status == BatchProcessorStatus.OK;
        }
    }
}

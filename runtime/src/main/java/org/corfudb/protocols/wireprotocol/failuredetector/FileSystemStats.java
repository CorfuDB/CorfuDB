package org.corfudb.protocols.wireprotocol.failuredetector;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@AllArgsConstructor
@EqualsAndHashCode
@Getter
@ToString
public class FileSystemStats {

    @NonNull
    private final PartitionAttributeStats partitionAttributeStats;

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
}

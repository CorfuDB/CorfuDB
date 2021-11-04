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
    private final ResourceQuotaStats quota;

    @NonNull
    private final PartitionAttrStat partitionAttr;

    @AllArgsConstructor
    @EqualsAndHashCode
    @Getter
    @ToString
    public static class ResourceQuotaStats {
        private final long limit;
        private final long used;

        public boolean isExceeded() {
            return used > limit;
        }

        public boolean isNotExceeded(){
            return !isExceeded();
        }

        public long available() {
            return limit - used;
        }
    }

    @AllArgsConstructor
    @Getter
    @EqualsAndHashCode
    @ToString
    public static class PartitionAttrStat {
        private final boolean readOnly;

        public boolean isWritable() {
            return !readOnly;
        }
    }
}

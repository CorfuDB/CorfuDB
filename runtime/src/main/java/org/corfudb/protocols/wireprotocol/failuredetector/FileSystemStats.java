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

    @AllArgsConstructor
    @EqualsAndHashCode
    @Getter
    @ToString
    public static class ResourceQuotaStats {
        private final long limit;
        private final long used;

        public boolean isExceeded(){
            return used > limit;
        }

        public long available() {
            return limit - used;
        }
    }
}

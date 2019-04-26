package org.corfudb.runtime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import org.corfudb.protocols.logprotocol.ISMREntryLocator;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Data
@AllArgsConstructor
public class GarbageMark {
    @AllArgsConstructor
    static public enum GarbageMarkType {
        PREFIXTRIM(0),
        SPARSETRIM(1);

        @Getter
        private final int type;

        public byte asByte() {
            return (byte) type;
        }
    }

    GarbageMarkType type;
    UUID streamId;
    ISMREntryLocator locator;
    CompletableFuture<Void> cf;
}

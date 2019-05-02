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
        SHUTDOWN(0),
        PREFIXTRIM(1),
        SPARSETRIM(2);

        @Getter
        private final int type;
    }

    private final GarbageMarkType type;
    private final UUID streamId;
    private final ISMREntryLocator locator;
    private final CompletableFuture<Void> cf;

    public static GarbageMark SHUTDOWN = new GarbageMark(GarbageMarkType.SHUTDOWN, null, null, null);
}

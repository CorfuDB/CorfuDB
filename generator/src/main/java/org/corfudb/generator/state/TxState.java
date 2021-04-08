package org.corfudb.generator.state;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.operations.Operation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TxState {

    @Builder
    @ToString
    public static class TxContext {
        private final KeysState.ThreadName id;
        private final String clientId;
        @NonNull
        private final Operation.Type opType;
        @NonNull
        @Builder.Default
        private final Keys.Version version = Keys.Version.noVersion();

        @NonNull
        @Builder.Default
        private final Keys.Version startTxVersion = Keys.Version.noVersion();

        @Builder.Default
        private final ConcurrentMap<Keys.FullyQualifiedKey, String> values = new ConcurrentHashMap<>();

        private final boolean terminated;
    }

    public enum TxStatus {
        START, END, ABORTED
    }
}

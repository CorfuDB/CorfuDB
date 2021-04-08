package org.corfudb.generator.state;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.operations.Operation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Builder
public class TxState {

    private final ConcurrentMap<KeysState.ThreadName, TxContext> state = new ConcurrentHashMap<>();

    public TxContext get(KeysState.ThreadName transactionId) {
        return state.get(transactionId);
    }

    @Builder
    @ToString
    public static class TxContext {
        @NonNull
        private final KeysState.ThreadName id;
        private final String clientId;
        @NonNull
        private final Operation.Type opType;
        @NonNull
        @Builder.Default
        @Setter
        @Getter
        private Keys.Version version = Keys.Version.noVersion();

        @NonNull
        @Builder.Default
        private final Keys.Version startTxVersion = Keys.Version.noVersion();

        @Builder.Default
        private final ConcurrentMap<Keys.FullyQualifiedKey, String> values = new ConcurrentHashMap<>();

        private final boolean terminated;

        public boolean contains(Keys.FullyQualifiedKey key) {
            return values.containsKey(key);
        }
    }

    public enum TxStatus {
        START, END, ABORTED
    }
}

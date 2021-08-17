package org.corfudb.generator.state;

import lombok.AllArgsConstructor;
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

    public void put(KeysState.ThreadName thread, TxContext context) {
        state.put(thread, context);
    }

    @Builder
    @ToString
    public static class TxContext {
        @Getter
        @NonNull
        private KeysState.SnapshotId snapshotId;

        @NonNull
        private final Operation.Type opType;

        @NonNull
        @Builder.Default
        private final Keys.Version startTxVersion = Keys.Version.noVersion();

        @Builder.Default
        private final ConcurrentMap<Keys.FullyQualifiedKey, String> values = new ConcurrentHashMap<>();

        @Getter
        @Setter
        private boolean terminated;

        public boolean contains(Keys.FullyQualifiedKey key) {
            return values.containsKey(key);
        }
    }

    @Getter
    @AllArgsConstructor
    public enum TxStatus {
        START("start"), END("end"), ABORTED("aborted");

        private final String status;
    }
}

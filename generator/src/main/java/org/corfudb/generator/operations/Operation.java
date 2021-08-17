package org.corfudb.generator.operations;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.val;
import org.corfudb.generator.correctness.Correctness;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.Keys.FullyQualifiedKey;
import org.corfudb.generator.state.State;

import java.util.Optional;
import java.util.UUID;

/**
 * A definition of a generic operation that the generator can execute.
 * <p>
 * Created by maithem on 7/14/17.
 */
public abstract class Operation {
    @Getter
    protected final State state;
    @Getter
    protected final Operation.Type opType;

    protected final Correctness correctness;

    public Operation(State state, Operation.Type operationType) {
        this.state = state;
        this.opType = operationType;
        this.correctness = new Correctness();
    }

    public abstract void execute();

    protected Keys.FullyQualifiedKey generateFqKey(State state) {
        return Keys.FullyQualifiedKey.builder()
                .tableId(state.getStreams().sample())
                .keyId(state.getKeys().sample())
                .build();
    }

    /**
     * Operation context keeps an operation state and data, and can be used by other parts of the application
     * without the "operation" itself to verify that the corfu database or table is in consistent state.
     */
    @Builder
    @Getter
    public static class Context {
        @NonNull
        private final FullyQualifiedKey fqKey;
        @NonNull
        @Builder.Default
        private Optional<String> val = Optional.empty();

        @Builder.Default
        private Keys.Version version = Keys.Version.noVersion();

        public OperationLogMessage getCorrectnessRecord(Type operationName) {
            return new OperationLogMessage(this, operationName);
        }
    }

    @AllArgsConstructor
    public static class OperationLogMessage implements Correctness.LogMessage {
        private final Context context;
        private final Type operationName;

        @Override
        public String getMessage() {
            String value = context.val.orElse(null);
            UUID streamId = context.fqKey.getTableId().getStreamId();
            String keyId = context.fqKey.getKeyId().getKey();
            return String.format("%s, %s:%s=%s", operationName.getOpType(), streamId, keyId, value);
        }
    }

    @AllArgsConstructor
    public enum Type {
        READ("Read"), REMOVE("Rm"), SLEEP("Sleep"), WRITE("Write"),
        TX_NESTED("TxNest"), TX_OPTIMISTIC("TxOpt"), TX_SNAPSHOT("TxSnap"), TX_WAW("TxWaw"),
        CHECKPOINT("Checkpoint");

        public boolean notTxOptimisticOrNestedOrSnapshot() {
            return !(this == TX_OPTIMISTIC || this == TX_NESTED || this == TX_SNAPSHOT);
        }

        @Getter
        private final String opType;
    }
}

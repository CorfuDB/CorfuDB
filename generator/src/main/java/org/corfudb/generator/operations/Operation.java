package org.corfudb.generator.operations;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.Keys.FullyQualifiedKey;
import org.corfudb.generator.distributions.Keys.KeyId;
import org.corfudb.generator.distributions.Streams.StreamId;
import org.corfudb.generator.state.State;

import java.util.Optional;

/**
 * A definition of a generic operation that the generator can execute.
 */
public abstract class Operation {
    protected final State state;
    @Getter
    protected final Operation.Type opType;

    public Operation(State state, Operation.Type operationType) {
        this.state = state;
        this.opType = operationType;
    }

    public abstract void execute();

    /**
     * Operation context keeps an operation state and data, and can be used by other parts of the application
     * without the "operation" itself to verify that the corfu database or table is in consistent state.
     */
    @Builder
    @Getter
    public static class Context {
        @NonNull
        private final StreamId streamId;
        @NonNull
        private final KeyId key;
        @NonNull
        @Builder.Default
        private final Optional<String> val = Optional.empty();

        @Setter
        private Keys.Version version;

        public String getCorrectnessRecord(String operationName) {
            String value = val.orElseThrow(()-> new IllegalStateException("Empty value for: " + getFqKey()));
            return String.format("%s, %s:%s=%s", operationName, streamId, key, value);
        }

        public FullyQualifiedKey getFqKey() {
            return  FullyQualifiedKey.builder().keyId(key).tableId(streamId).build();
        }
    }

    @AllArgsConstructor
    public enum Type {
        READ("Read"), REMOVE("Rm"), SLEEP("Sleep"), WRITE("Write"), 
        TX_NESTED("TxNest"), TX_OPTIMISTIC("TxOpt"), TX_SNAPSHOT("TxSnap"), TX_WAW("TxWaw");

        @Getter
        private final String opType;
    }
}

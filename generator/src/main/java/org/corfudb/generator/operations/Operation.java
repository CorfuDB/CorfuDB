package org.corfudb.generator.operations;

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
    protected final String shortName;

    public Operation(State state, String shortName) {
        this.state = state;
        this.shortName = shortName;
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
}

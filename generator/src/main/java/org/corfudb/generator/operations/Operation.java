package org.corfudb.generator.operations;

import lombok.Builder;
import lombok.Getter;
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

    @Builder
    @Getter
    public static class Context {
        private final StreamId streamId;
        private final KeyId key;
        private final Optional<String> val;

        @Setter
        private Keys.Version version;

        public String getCorrectnessRecord(String operationName) {
            return String.format("%s, %s:%s=%s", operationName, streamId, key, val.get());
        }

        public FullyQualifiedKey getFqKey() {
            return  FullyQualifiedKey.builder().keyId(key).tableId(streamId).build();
        }
    }
}

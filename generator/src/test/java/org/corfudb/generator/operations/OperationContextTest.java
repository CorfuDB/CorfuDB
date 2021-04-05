package org.corfudb.generator.operations;

import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.Streams;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OperationContextTest {

    @Test

    public void testCorrectnessRecordMissingValue() {
        Operation.Context ctx = Operation.Context.builder()
                .streamId(new Streams.StreamId(123))
                .key(new Keys.KeyId(333))
                .build();

        Assertions.assertThrows(IllegalStateException.class, () -> {
            ctx.getCorrectnessRecord("yay");
        });
    }
}
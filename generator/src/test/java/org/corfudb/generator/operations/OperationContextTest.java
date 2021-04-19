package org.corfudb.generator.operations;

import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.Keys.FullyQualifiedKey;
import org.corfudb.generator.distributions.Streams;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OperationContextTest {

    @Test

    public void testCorrectnessRecordMissingValue() {
        FullyQualifiedKey key = FullyQualifiedKey.builder()
                .tableId(new Streams.StreamId(123))
                .keyId(new Keys.KeyId(333))
                .build();

        Operation.Context ctx = Operation.Context.builder().fqKey(key).build();

        Assertions.assertThrows(IllegalStateException.class, () -> {
            ctx.getCorrectnessRecord("yay");
        });
    }
}
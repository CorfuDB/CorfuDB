package org.corfudb.generator.operations;

import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.Keys.FullyQualifiedKey;
import org.corfudb.generator.distributions.Streams.StreamId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OperationContextTest {

    @Test
    public void testCorrectnessRecordMissingValue() {
        StreamId tableId = new StreamId(123);
        FullyQualifiedKey key = FullyQualifiedKey.builder()
                .tableId(tableId)
                .keyId(new Keys.KeyId(333))
                .build();

        Operation.Context ctx = Operation.Context.builder().fqKey(key).build();

        //${OpType}, ${table_name}:${key}=${value}, ${}
        String expected = String.format("Read, %s:333=null", tableId.getStreamId());
        assertEquals(expected, ctx.getCorrectnessRecord(Operation.Type.READ));
    }
}
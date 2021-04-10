package org.corfudb.generator.verification;

import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.Streams;
import org.corfudb.generator.operations.ReadOperation;
import org.corfudb.generator.operations.WriteOperation;
import org.corfudb.generator.state.State;
import org.corfudb.generator.state.State.CorfuTablesGenerator;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.junit.jupiter.api.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class VerificationTest {

    public static void main(String[] args) {
        System.out.println("yay");
    }

    @Test
    public void testReadWriteAndVerification() {
        Streams streams = new Streams(50);
        Keys keys = new Keys(50);
        streams.populate();
        keys.populate();

        State state = new State(streams, keys);
        CorfuRuntime rt = mock(CorfuRuntime.class);

        CorfuTable<String, String> corfuTableMock = mock(CorfuTable.class);

        CorfuTablesGenerator tablesManager = new CorfuTablesGenerator(rt, streams);
        CorfuTablesGenerator tablesManagerMock = spy(tablesManager);
        doReturn(corfuTableMock).when(tablesManagerMock).getMap(any(Streams.StreamId.class));

        WriteOperation write = new WriteOperation(state, tablesManager);
        write.execute();

        ReadOperation read = new ReadOperation(state, tablesManagerMock);
        ReadOperationVerification verification = new ReadOperationVerification(state, read.getContext());

        verification.verify();
    }

}
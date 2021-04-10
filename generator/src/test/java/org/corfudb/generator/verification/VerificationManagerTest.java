package org.corfudb.generator.verification;

import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.Streams;
import org.corfudb.generator.operations.ReadOperation;
import org.corfudb.generator.state.State;
import org.corfudb.generator.state.State.CorfuTablesGenerator;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

class VerificationTest {

    public static void main(String[] args) {
        System.out.println("yay");
    }

    @Test
    public void testReadOperationAndVerification() {
        Streams streams = new Streams(50);
        Keys keys = new Keys(50);
        streams.populate();
        keys.populate();

        State state = new State(streams, keys);
        CorfuRuntime rt = mock(CorfuRuntime.class);
        CorfuTablesGenerator tablesManager = new CorfuTablesGenerator(rt, streams);
        CorfuTablesGenerator tablesManagerMock = spy(tablesManager);

        ReadOperation read = new ReadOperation(state, tablesManagerMock);
        ReadOperationVerification verification = new ReadOperationVerification(state, read.getContext());

        verification.verify();
    }

}
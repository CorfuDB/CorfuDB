package org.corfudb.generator.verification;

import org.corfudb.generator.Correctness;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.Streams;
import org.corfudb.generator.operations.ReadOperation;
import org.corfudb.generator.operations.WriteOperation;
import org.corfudb.generator.state.CorfuTablesGenerator;
import org.corfudb.generator.state.State;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class VerificationTest {

    @Test
    public void testReadWriteAndVerification() {
        Streams streams = new Streams(1);
        Keys keys = new Keys(1);
        streams.populate();
        keys.populate();

        State state = new State(streams, keys);
        CorfuRuntime rt = mock(CorfuRuntime.class);

        CorfuTablesGenerator tablesManager = new CorfuTablesGenerator(rt, streams);
        CorfuTablesGenerator tablesManagerMock = spy(tablesManager);
        setupTableManager(tablesManagerMock);

        tablesManagerMock.startOptimisticTx();

        Correctness correctness = new Correctness();

        WriteOperation write = new WriteOperation(state, tablesManagerMock, correctness);
        write.execute();

        ReadOperation read = new ReadOperation(state, tablesManagerMock, correctness);
        read.execute();

        tablesManagerMock.stopTx();

        ReadOperationVerification verification = new ReadOperationVerification(state, read.getContext());
        Assertions.assertTrue(verification.verify());
    }

    private void setupTableManager(CorfuTablesGenerator tablesManagerMock) {
        Map<String, String> map = new HashMap<>();
        CorfuTable<String, String> corfuTableMock = mock(CorfuTable.class);
        Answer<String> putMock = invocation -> {
            String key = invocation.getArgumentAt(0, String.class);
            String value = invocation.getArgumentAt(1, String.class);

            map.put(key, value);

            return value;
        };

        Answer<String> getMock = invocation -> {
            String key = invocation.getArgumentAt(0, String.class);
            return map.get(key);
        };

        when(corfuTableMock.put(anyString(), anyString())).thenAnswer(putMock);
        when(corfuTableMock.get(anyString())).thenAnswer(getMock);

        doReturn(corfuTableMock).when(tablesManagerMock).getMap(any(Streams.StreamId.class));
        doNothing().when(tablesManagerMock).startOptimisticTx();
        doReturn(1L).when(tablesManagerMock).stopTx();
        doReturn(true).when(tablesManagerMock).isInTransaction();
    }
}
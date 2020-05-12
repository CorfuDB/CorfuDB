package org.corfudb;

import org.corfudb.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.junit.Test;
import org.mockito.Mockito;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.times;

public class CorfuLogReplicationRuntimeTest {

    @Test
    public void testReplicatedStreamTableCreation() {
    }

    @Test
    public void testReplicatedStreamTableVersionMismatch() {
    }
}

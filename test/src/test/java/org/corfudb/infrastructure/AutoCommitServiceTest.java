package org.corfudb.infrastructure;

import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by WenbinZhu on 9/30/19.
 */
public class AutoCommitServiceTest extends AbstractViewTest {

    /**
     * Generates and bootstraps a 3 node cluster in disk mode.
     * Shuts down the management servers of the 3 nodes.
     *
     * @return The generated layout.
     */
    private Layout setup3NodeCluster() {
        ServerContext sc0 = new ServerContextBuilder()
                .setSingle(false)
                .setServerRouter(new TestServerRouter(SERVERS.PORT_0))
                .setPort(SERVERS.PORT_0)
                .setMemory(false)
                .setLogPath(com.google.common.io.Files.createTempDir().getAbsolutePath())
                .build();
        ServerContext sc1 = new ServerContextBuilder()
                .setSingle(false)
                .setServerRouter(new TestServerRouter(SERVERS.PORT_1))
                .setPort(SERVERS.PORT_1)
                .setMemory(false)
                .setLogPath(com.google.common.io.Files.createTempDir().getAbsolutePath())
                .build();
        ServerContext sc2 = new ServerContextBuilder()
                .setSingle(false)
                .setServerRouter(new TestServerRouter(SERVERS.PORT_2))
                .setPort(SERVERS.PORT_2)
                .setMemory(false)
                .setLogPath(com.google.common.io.Files.createTempDir().getAbsolutePath())
                .build();

        addServer(SERVERS.PORT_0, sc0);
        addServer(SERVERS.PORT_1, sc1);
        addServer(SERVERS.PORT_2, sc2);
        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .setReplicationMode(Layout.ReplicationMode.CHAIN_REPLICATION)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build();

        bootstrapAllServers(l);

        // Shutdown management servers.
        getManagementServer(SERVERS.PORT_0).shutdown();
        getManagementServer(SERVERS.PORT_1).shutdown();
        getManagementServer(SERVERS.PORT_2).shutdown();

        return l;
    }

    private LogData getLogData(TokenResponse token, byte[] payload) {
        LogData ld = new LogData(DataType.DATA, payload);
        ld.useToken(token);
        return ld;
    }

    private LogData read(CorfuRuntime runtime, long address, String server) throws Exception {
        return runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(server)
                .read(address).get().getAddresses().get(address);
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testAutoCommitHoles() throws Exception {
        CorfuRuntime runtime = getRuntime(setup3NodeCluster()).connect();

        final int numIter = 100;
        Set<Long> noWriteHoles = new HashSet<>(Arrays.asList(15L, 25L, 45L, 85L));
        Set<Long> partialWriteHoles = new HashSet<>(Arrays.asList(55L, 75L));

        for (long i = 0; i < numIter; i++) {
            TokenResponse token = runtime.getSequencerView().next();
            if (noWriteHoles.contains(i)) {
                // Write nothing to create a hole on all log units.
            } else if (partialWriteHoles.contains(i)) {
                // Write to head log unit to create a partial write hole.
                runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_0)
                        .write(getLogData(token, "partial write".getBytes())).get();
            } else {
                runtime.getAddressSpaceView().write(token, "Test Payload".getBytes());
            }
        }

        AutoCommitService autoCommitService =
                getManagementServer(SERVERS.PORT_0).getManagementAgent().getAutoCommitService();
        // First invocation would only set the next commit end to the current log tail.
        autoCommitService.runAutoCommit();

        for (long i = 0; i < numIter; i++) {
            if (noWriteHoles.contains(i) || partialWriteHoles.contains(i)) {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_2).getType()).isEqualTo(DataType.EMPTY);
            } else {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_2).getType()).isEqualTo(DataType.DATA);
            }
        }

        // Second invocation would do actual commit.
        autoCommitService.runAutoCommit();

        for (long i = 0; i < numIter; i++) {
            if (noWriteHoles.contains(i)) {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_2).getType()).isEqualTo(DataType.HOLE);
            } else {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_2).getType()).isEqualTo(DataType.DATA);
            }
        }

        assertThat(runtime.getLayoutView().getRuntimeLayout()
                .getLogUnitClient(SERVERS.ENDPOINT_0).getCommittedTail().get()).isEqualTo(numIter - 1);
        assertThat(runtime.getLayoutView().getRuntimeLayout()
                .getLogUnitClient(SERVERS.ENDPOINT_1).getCommittedTail().get()).isEqualTo(numIter - 1);
        assertThat(runtime.getLayoutView().getRuntimeLayout()
                .getLogUnitClient(SERVERS.ENDPOINT_2).getCommittedTail().get()).isEqualTo(numIter - 1);
    }
}

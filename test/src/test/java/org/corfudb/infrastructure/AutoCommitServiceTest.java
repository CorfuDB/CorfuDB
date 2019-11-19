package org.corfudb.infrastructure;

import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static junit.framework.TestCase.fail;

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

    private void write(CorfuRuntime runtime, int numIter,
                       Set<Long> noWriteHoles, Set<Long> partialWriteHoles) throws Exception {
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
    }

    private LogData read(CorfuRuntime runtime, long address, String server) throws Exception {
        return runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(server)
                .read(address).get().getAddresses().get(address);
    }

    /**
     * Test the auto commit service can commit all the holes and consolidate the log prefix.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testAutoCommitHoles() throws Exception {
        CorfuRuntime runtime = getRuntime(setup3NodeCluster()).connect();

        final int numIter = 600;
        Set<Long> noWriteHoles = new HashSet<>(Arrays.asList(15L, 245L, 450L, 575L));
        Set<Long> partialWriteHoles = new HashSet<>(Arrays.asList(55L, 175L, 525L));

        write(runtime, numIter, noWriteHoles, partialWriteHoles);

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

    /**
     * Test auto commit does not run on a non-sequencer node.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testAutoCommitNotRunningOnNonSequencer() throws Exception {
        CorfuRuntime runtime = getRuntime(setup3NodeCluster()).connect();

        final int numIter = 100;
        Set<Long> noWriteHoles = new HashSet<>(Arrays.asList(15L, 25L, 45L, 85L));
        Set<Long> partialWriteHoles = new HashSet<>(Arrays.asList(55L, 75L));

        write(runtime, numIter, noWriteHoles, partialWriteHoles);

        // Try to invoke auto commit on non-sequencer node, which should be no-op
        AutoCommitService autoCommitService =
                getManagementServer(SERVERS.PORT_1).getManagementAgent().getAutoCommitService();

        // First invocation would only set the next commit end to the current log tail.
        autoCommitService.runAutoCommit();
        for (long i = 0; i < numIter; i++) {
            if (noWriteHoles.contains(i) || partialWriteHoles.contains(i)) {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_2).getType()).isEqualTo(DataType.EMPTY);
            } else {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_2).getType()).isEqualTo(DataType.DATA);
            }
        }

        // Auto commit should be no-op as this is invoked on a non-sequencer node.
        autoCommitService.runAutoCommit();
        for (long i = 0; i < numIter; i++) {
            if (noWriteHoles.contains(i) || partialWriteHoles.contains(i)) {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_2).getType()).isEqualTo(DataType.EMPTY);
            } else {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_2).getType()).isEqualTo(DataType.DATA);
            }
        }

        // Invoke auto commit on sequencer node, which should do the job.
        autoCommitService = getManagementServer(SERVERS.PORT_0).getManagementAgent().getAutoCommitService();

        // First invocation would only set the next commit end to the current log tail.
        autoCommitService.runAutoCommit();
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

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testSequencerChangeDuringAutoCommit() throws Exception {
        CorfuRuntime runtime = getRuntime(setup3NodeCluster()).connect();

        final int numIter = 600;
        Set<Long> noWriteHoles = new HashSet<>(Arrays.asList(15L, 245L, 450L, 575L));
        Set<Long> partialWriteHoles = new HashSet<>(Arrays.asList(55L, 175L, 525L));

        write(runtime, numIter, noWriteHoles, partialWriteHoles);

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

        ExecutorService executor = Executors.newSingleThreadExecutor();

        addServerRule(SERVERS.PORT_2, new TestRule().matches(msg -> {
            if (msg.getMsgType().equals(CorfuMsgType.WRITE_OK)) {
                try {
                    Layout layout = runtime.getLayoutView().getLayout();
                    if (layout.getPrimarySequencer().equals(SERVERS.ENDPOINT_1)) {
                        return true;
                    }
                    Layout newLayout = new Layout(layout);
                    newLayout.setSequencers(Arrays.asList(SERVERS.ENDPOINT_1, SERVERS.ENDPOINT_0, SERVERS.ENDPOINT_2));
                    newLayout.nextEpoch();
                    runtime.getLayoutView().getRuntimeLayout(newLayout).sealMinServerSet();
                    runtime.getLayoutView().updateLayout(newLayout, 1L);
                    runtime.getLayoutManagementView().reconfigureSequencerServers(layout, newLayout, true);
                    runtime.invalidateLayout();

                    AutoCommitService newAutoCommitService
                            = getManagementServer(SERVERS.PORT_1).getManagementAgent().getAutoCommitService();
                    // Run auto commit on new sequencer.
                    newAutoCommitService.runAutoCommit();
                    executor.submit(newAutoCommitService::runAutoCommit);
                } catch (Exception e) {
                    fail();
                }
            }

            return true;
        }));

        // Continue auto commit on the old sequencer while the new auto committer is on going.
        autoCommitService.runAutoCommit();

        executor.shutdown();
        executor.awaitTermination(20, TimeUnit.SECONDS);

        // After both auto committer finished the holes should be filled.
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

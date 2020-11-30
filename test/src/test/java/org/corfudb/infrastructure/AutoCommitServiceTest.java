package org.corfudb.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutBuilder;
import org.corfudb.test.TestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.TestCase.fail;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by WenbinZhu on 5/22/20.
 */
@Slf4j
public class AutoCommitServiceTest extends AbstractViewTest {

    private Random rand = new Random();

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
                .setCacheSizeHeapRatio("0.0")
                .setLogPath(com.google.common.io.Files.createTempDir().getAbsolutePath())
                .build();
        ServerContext sc1 = new ServerContextBuilder()
                .setSingle(false)
                .setServerRouter(new TestServerRouter(SERVERS.PORT_1))
                .setPort(SERVERS.PORT_1)
                .setMemory(false)
                .setCacheSizeHeapRatio("0.0")
                .setLogPath(com.google.common.io.Files.createTempDir().getAbsolutePath())
                .build();
        ServerContext sc2 = new ServerContextBuilder()
                .setSingle(false)
                .setServerRouter(new TestServerRouter(SERVERS.PORT_2))
                .setPort(SERVERS.PORT_2)
                .setMemory(false)
                .setCacheSizeHeapRatio("0.0")
                .setLogPath(com.google.common.io.Files.createTempDir().getAbsolutePath())
                .build();

        addServer(SERVERS.PORT_0, sc0);
        addServer(SERVERS.PORT_1, sc1);
        addServer(SERVERS.PORT_2, sc2);
        Layout l = new TestLayoutBuilder()
                .setEpoch(0L)
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

    private void write(CorfuRuntime runtime, long start, long end,
                       Set<Long> noWriteHoles, Set<Long> partialWriteHoles) throws Exception {
        for (long i = start; i < end; i++) {
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

    private void generateHoles(long start, long end, int numNoWrite, int numPartialWrite,
                               Set<Long> noWriteHoles, Set<Long> partialWriteHoles) {
        rand.longs(numNoWrite, start, end).forEach(noWriteHoles::add);
        rand.longs(numPartialWrite, start, end).forEach(partialWriteHoles::add);
    }

    /**
     * Test the auto commit service can commit all the holes and consolidate the log prefix.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testAutoCommitHoles() throws Exception {
        CorfuRuntime runtime = getRuntime(setup3NodeCluster()).connect();

        int batchSize = AutoCommitService.COMMIT_BATCH_SIZE;
        final long numIter = 2 * batchSize - 1;
        Set<Long> noWriteHoles = new HashSet<>();
        Set<Long> partialWriteHoles = new HashSet<>();
        generateHoles(0, batchSize, 3, 2, noWriteHoles, partialWriteHoles);
        generateHoles(batchSize, numIter, 2, 3, noWriteHoles, partialWriteHoles);

        write(runtime, 0L, numIter, noWriteHoles, partialWriteHoles);

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

        assertThat(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_0)
                .getCommittedTail().get()).isEqualTo(numIter - 1);
        assertThat(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_1)
                .getCommittedTail().get()).isEqualTo(numIter - 1);
        assertThat(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_2)
                .getCommittedTail().get()).isEqualTo(numIter - 1);
    }

    /**
     * Test auto commit does not run on a non-sequencer node.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testAutoCommitNotRunningOnNonSequencer() throws Exception {
        CorfuRuntime runtime = getRuntime(setup3NodeCluster()).connect();

        final long numIter = AutoCommitService.COMMIT_BATCH_SIZE - 1;
        Set<Long> noWriteHoles = new HashSet<>();
        Set<Long> partialWriteHoles = new HashSet<>();
        generateHoles(0, numIter, 4, 2, noWriteHoles, partialWriteHoles);

        write(runtime, 0L, numIter, noWriteHoles, partialWriteHoles);

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

        assertThat(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_0)
                .getCommittedTail().get()).isEqualTo(numIter - 1);
        assertThat(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_1)
                .getCommittedTail().get()).isEqualTo(numIter - 1);
        assertThat(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_2)
                .getCommittedTail().get()).isEqualTo(numIter - 1);
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testSequencerChangeDuringAutoCommit() throws Exception {
        CorfuRuntime runtime = getRuntime(setup3NodeCluster()).connect();

        int batchSize = AutoCommitService.COMMIT_BATCH_SIZE;
        final long numIter = 2 * batchSize - 1;
        Set<Long> noWriteHoles = new HashSet<>();
        Set<Long> partialWriteHoles = new HashSet<>();
        // Use numIter - 1 to prevent last address being a no-write hole
        // which regresses sequencer token after sequencer failover.
        generateHoles(0, batchSize, 2, 2, noWriteHoles, partialWriteHoles);
        generateHoles(batchSize, numIter - 1, 3, 3, noWriteHoles, partialWriteHoles);

        write(runtime, 0L, numIter, noWriteHoles, partialWriteHoles);

        for (long i = 0; i < numIter; i++) {
            if (noWriteHoles.contains(i) || partialWriteHoles.contains(i)) {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_2).getType()).isEqualTo(DataType.EMPTY);
            } else {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_2).getType()).isEqualTo(DataType.DATA);
            }
        }

        AutoCommitService autoCommitService =
                getManagementServer(SERVERS.PORT_0).getManagementAgent().getAutoCommitService();
        // First invocation would only set the next commit end to the current log tail.
        autoCommitService.runAutoCommit();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicBoolean ruleExecuted = new AtomicBoolean(false);

        addServerRule(SERVERS.PORT_2, new TestRule().responseMatches(msg -> {
            if (msg.getPayload().getPayloadCase().equals(ResponsePayloadMsg.PayloadCase.INSPECT_ADDRESSES_RESPONSE)) {
                try {
                    if (ruleExecuted.getAndSet(true)) {
                        return true;
                    }
                    Layout layout = runtime.getLayoutView().getLayout();
                    Layout newLayout = new Layout(layout);
                    newLayout.setSequencers(Arrays.asList(SERVERS.ENDPOINT_1, SERVERS.ENDPOINT_0, SERVERS.ENDPOINT_2));
                    newLayout.nextEpoch();
                    runtime.getLayoutView().getRuntimeLayout(newLayout).sealMinServerSet();
                    runtime.getLayoutView().updateLayout(newLayout, 1L);
                    runtime.getLayoutManagementView().reconfigureSequencerServers(layout, newLayout, true);
                    TestUtils.waitForLayoutChange(l -> l.getEpoch() == newLayout.getEpoch(), runtime);

                    AutoCommitService newAutoCommitService
                            = getManagementServer(SERVERS.PORT_1).getManagementAgent().getAutoCommitService();
                    TestUtils.waitForLayoutChange(
                            l -> l.getEpoch() == newLayout.getEpoch(), newAutoCommitService.getCorfuRuntime());
                    // Run auto commit on new sequencer.
                    // First invocation would only set the next commit end to the current log tail.
                    newAutoCommitService.runAutoCommit();
                    executor.submit(newAutoCommitService::runAutoCommit);
                } catch (Exception e) {
                    fail();
                }
            }

            return true;
        }));

        autoCommitService.runAutoCommit();

        executor.shutdown();
        executor.awaitTermination(20, TimeUnit.SECONDS);

        assertThat(runtime.getLayoutView().getLayout().getEpoch()).isEqualTo(1L);

        // After both auto committer finished the holes should be filled.
        for (long i = 0; i < numIter; i++) {
            if (noWriteHoles.contains(i)) {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_2).getType()).isEqualTo(DataType.HOLE);
            } else {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_2).getType()).isEqualTo(DataType.DATA);
            }
        }

        assertThat(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_0)
                .getCommittedTail().get()).isEqualTo(numIter - 1);
        assertThat(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_1)
                .getCommittedTail().get()).isEqualTo(numIter - 1);
        assertThat(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_2)
                .getCommittedTail().get()).isEqualTo(numIter - 1);
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testCatchingUpCommittedTailWithTrimMark() throws Exception {
        CorfuRuntime runtime = getRuntime(setup3NodeCluster()).connect();

        final long numIter = AutoCommitService.COMMIT_BATCH_SIZE - 1;
        Set<Long> noWriteHoles = new HashSet<>();
        Set<Long> partialWriteHoles = new HashSet<>();
        generateHoles(0, numIter, 2, 3, noWriteHoles, partialWriteHoles);

        write(runtime, 0L, numIter, noWriteHoles, partialWriteHoles);

        AutoCommitService autoCommitService =
                getManagementServer(SERVERS.PORT_0).getManagementAgent().getAutoCommitService();
        // First invocation would only set the next commit end to the current log tail.
        autoCommitService.runAutoCommit();

        Layout layout = runtime.getLayoutView().getLayout();
        Token oldTrimMark = Token.of(layout.getEpoch(), 100);
        Token newTrimMark = Token.of(layout.getEpoch(), 200);
        // Head and middle chain has old trim mark, tail chain has new trim mark.
        runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_0).prefixTrim(oldTrimMark).join();
        runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_1).prefixTrim(oldTrimMark).join();
        runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_2).prefixTrim(newTrimMark).join();

        autoCommitService.runAutoCommit();

        assertThat(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_0)
                .getTrimMark().get()).isEqualTo(newTrimMark.getSequence() + 1L);
        assertThat(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_1)
                .getTrimMark().get()).isEqualTo(newTrimMark.getSequence() + 1L);
        assertThat(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_2)
                .getTrimMark().get()).isEqualTo(newTrimMark.getSequence() + 1L);

        assertThat(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_0)
                .getCommittedTail().get()).isEqualTo(numIter - 1L);
        assertThat(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_1)
                .getCommittedTail().get()).isEqualTo(numIter - 1L);
        assertThat(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_2)
                .getCommittedTail().get()).isEqualTo(numIter - 1L);

        for (long i = 0; i < numIter; i++) {
            if (i <= newTrimMark.getSequence()) {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_2).getType()).isEqualTo(DataType.TRIMMED);
            } else if (noWriteHoles.contains(i)) {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_2).getType()).isEqualTo(DataType.HOLE);
            } else {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_2).getType()).isEqualTo(DataType.DATA);
            }
        }
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testSegmentChangeDuringAutoCommit() throws Exception {
        CorfuRuntime runtime = getRuntime(setup3NodeCluster()).connect();
        Layout layout = runtime.getLayoutView().getLayout();

        int batchSize = AutoCommitService.COMMIT_BATCH_SIZE;
        final long numIter = 2 * batchSize - 1;
        Set<Long> noWriteHoles = new HashSet<>();
        Set<Long> partialWriteHoles = new HashSet<>();
        generateHoles(0, batchSize, 3, 4, noWriteHoles, partialWriteHoles);
        generateHoles(batchSize, numIter, 2, 3, noWriteHoles, partialWriteHoles);

        write(runtime, 0L, numIter, noWriteHoles, partialWriteHoles);

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

        // Remove and add log unit server 2 to split the segment.
        addClientRule(autoCommitService.getCorfuRuntime(), new TestRule().requestMatches(msg -> {
            if (msg.getPayload().getPayloadCase().equals(RequestPayloadMsg.PayloadCase.UPDATE_COMMITTED_TAIL_REQUEST)) {
                if (runtime.getLayoutView().getLayout().getSegments().size() > 1) {
                    return true;
                }

                LayoutBuilder builder = new LayoutBuilder(layout);
                builder.removeLogunitServer(SERVERS.ENDPOINT_2);
                builder.addLogunitServer(0, numIter - 1, SERVERS.ENDPOINT_2);
                builder.setEpoch(layout.getEpoch() + 1);
                Layout newLayout = builder.build();

                runtime.getLayoutView().getRuntimeLayout(newLayout).sealMinServerSet();
                runtime.getLayoutView().updateLayout(newLayout, 1L);
                runtime.getLayoutManagementView().reconfigureSequencerServers(layout, newLayout, false);
                TestUtils.waitForLayoutChange(l -> l.getEpoch() >= newLayout.getEpoch(), runtime);
            }

            return true;
        }));

        // Second invocation would do actual commit.
        autoCommitService.runAutoCommit();
        clearClientRules(autoCommitService.getCorfuRuntime());

        // New committed tail should only be set on fully redundant log units: server 0 and 1.
        assertThat(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_0)
                .getCommittedTail().get()).isEqualTo(numIter - 1);
        assertThat(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_1)
                .getCommittedTail().get()).isEqualTo(numIter - 1);
        assertThat(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_2)
                .getCommittedTail().get()).isEqualTo(Address.NON_ADDRESS);

        for (long i = 0; i < numIter; i++) {
            if (noWriteHoles.contains(i)) {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_1).getType()).isEqualTo(DataType.HOLE);
            } else {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_1).getType()).isEqualTo(DataType.DATA);
            }
        }
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testPrefixTrimDuringCommitCycle() throws Exception {
        CorfuRuntime runtime = getRuntime(setup3NodeCluster()).connect();

        int batchSize = AutoCommitService.COMMIT_BATCH_SIZE;
        final long start = 0L, firstEnd = batchSize / 2;
        Set<Long> firstNoWriteHoles = new HashSet<>();
        Set<Long> firstPartialWriteHoles = new HashSet<>();
        generateHoles(0, firstEnd, 2, 2, firstNoWriteHoles, firstPartialWriteHoles);

        write(runtime, start, firstEnd, firstNoWriteHoles, firstPartialWriteHoles);

        AutoCommitService autoCommitService =
                getManagementServer(SERVERS.PORT_0).getManagementAgent().getAutoCommitService();
        // First invocation would only set the next commit end to the current log tail.
        autoCommitService.runAutoCommit();

        // Generate holes after trim address to create TrimmedException and retry.
        final long trimAddress = rand.longs(1, firstEnd, batchSize).iterator().next();
        final long secondEnd = 2 * batchSize - 1;
        Set<Long> secondNoWriteHoles = new HashSet<>();
        Set<Long> secondPartialWriteHoles = new HashSet<>();
        generateHoles(trimAddress + 1L, secondEnd, 3, 4, secondNoWriteHoles, secondPartialWriteHoles);

        write(runtime, firstEnd, secondEnd, secondNoWriteHoles, secondPartialWriteHoles);

        // Commit up to address firstEnd.
        autoCommitService.runAutoCommit();

        for (long i = start; i < firstEnd; i++) {
            if (firstNoWriteHoles.contains(i)) {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_2).getType()).isEqualTo(DataType.HOLE);
            } else {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_2).getType()).isEqualTo(DataType.DATA);
            }
        }

        assertThat(runtime.getAddressSpaceView().getCommittedTail()).isEqualTo(firstEnd - 1);

        long epoch = runtime.getLayoutView().getLayout().getEpoch();
        AtomicBoolean ruleExecuted = new AtomicBoolean(false);

        addClientRule(autoCommitService.getCorfuRuntime(), new TestRule().requestMatches(msg -> {
            if (msg.getPayload().getPayloadCase().equals(RequestPayloadMsg.PayloadCase.INSPECT_ADDRESSES_REQUEST)) {
                if (ruleExecuted.getAndSet(true)) {
                    return true;
                }
                runtime.getAddressSpaceView().prefixTrim(Token.of(epoch, trimAddress));
            }
            return true;
        }));

        // Commit till end, should see TrimmedException and then retry and succeed.
        autoCommitService.runAutoCommit();
        clearClientRules(autoCommitService.getCorfuRuntime());

        for (long i = firstEnd; i < secondEnd; i++) {
            if (i <= trimAddress) {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_2).getType()).isEqualTo(DataType.TRIMMED);
            } else if (secondNoWriteHoles.contains(i)) {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_2).getType()).isEqualTo(DataType.HOLE);
            } else {
                assertThat(read(runtime, i, SERVERS.ENDPOINT_2).getType()).isEqualTo(DataType.DATA);
            }
        }

        assertThat(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_0)
                .getCommittedTail().get()).isEqualTo(secondEnd - 1);
        assertThat(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_1)
                .getCommittedTail().get()).isEqualTo(secondEnd - 1);
        assertThat(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_2)
                .getCommittedTail().get()).isEqualTo(secondEnd - 1);
    }
}

package org.corfudb.integration;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.CorfuTestParameters;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.Sleep;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertEquals;

@Slf4j
@SuppressWarnings("checkstyle:magicnumber")
public class StateTransferIT extends AbstractIT {

    public static final int FIRST_NODE_ID = 0;
    public static final int SECOND_NODE_ID = 1;

    private static String corfuSingleNodeHost;
    private final int basePort = 9000;
    private final int retries = 10;
    private CorfuRuntime firstRuntime;
    private CorfuRuntime secondRuntime;
    private CorfuRuntime writerRuntime;

    public StateTransferIT() {
        PARAMETERS = new CorfuTestParameters(Duration.ofMinutes(5));
    }

    private String getServerEndpoint(int port) {
        return corfuSingleNodeHost + ":" + port;
    }

    private Layout getLayout(int numNodes) {
        List<String> servers = new ArrayList<>();

        for (int x = 0; x < numNodes; x++) {
            String serverAddress = getServerEndpoint(basePort + x);
            servers.add(serverAddress);
        }

        return new Layout(
                new ArrayList<>(servers),
                new ArrayList<>(servers),
                Collections.singletonList(new Layout.LayoutSegment(
                        Layout.ReplicationMode.CHAIN_REPLICATION,
                        0L,
                        -1L,
                        Collections.singletonList(new Layout.LayoutStripe(servers)))),
                0L,
                UUID.randomUUID());
    }

    @Before
    public void loadProperties() {
        corfuSingleNodeHost = (String) PROPERTIES.get("corfuSingleNodeHost");
    }

    @After
    public void tearDown() {
        if (firstRuntime != null) {
            firstRuntime.shutdown();
        }
        if (secondRuntime != null) {
            secondRuntime.shutdown();
        }
        if (writerRuntime != null) {
            writerRuntime.shutdown();
        }
    }

    @Test
    public void verifyStateTransferWithChainHeadFailure() throws Exception {
        verifyStateTransferWithNodeFailure(FIRST_NODE_ID);
    }

    @Test
    public void verifyStateTransferWithChainTailFailure() throws Exception {
        verifyStateTransferWithNodeFailure(SECOND_NODE_ID);
    }

    @Test
    public void verifyStateTransferWithChainHeadRestart() throws Exception {
        verifyStateTransferWithNodeRestart(FIRST_NODE_ID);
    }

    @Test
    public void verifyStateTransferWithChainTailRestart() throws Exception {
        verifyStateTransferWithNodeRestart(SECOND_NODE_ID);
    }

    /**
     * A cluster of two nodes is started - 9000, 9001.
     * Then a block of data entries is written to the cluster.
     * 1 node - 9002 is added to the cluster, and triggers parallel transfer from two nodes.
     * Fail a node during transfer to verify it does not fail the transfer process.
     * Finally, the addition of node 9002 in the layout is verified.
     *
     * @throws Exception error
     */
    private void verifyStateTransferWithNodeFailure(int killNode) throws Exception {
        final int PORT_0 = 9000;
        final int PORT_1 = 9001;
        final int PORT_2 = 9002;
        final Duration timeout = Duration.ofMinutes(5);
        final Duration pollPeriod = Duration.ofSeconds(5);
        final int workflowNumRetry = 3;
        final int nodesCount = 3;

        Process corfuServer_1 = runPersistentServer(corfuSingleNodeHost, PORT_0, false);
        Process corfuServer_2 = runPersistentServer(corfuSingleNodeHost, PORT_1, false);
        Process corfuServer_3 = runPersistentServer(corfuSingleNodeHost, PORT_2, false);

        List<Process> corfuServers = Arrays.asList(corfuServer_1, corfuServer_2, corfuServer_3);

        // bootstrap cluster with 2 nodes
        final Layout twoNodeLayout = getLayout(2);
        BootstrapUtil.bootstrap(twoNodeLayout, retries, PARAMETERS.TIMEOUT_SHORT);

        firstRuntime = createDefaultRuntime();

        writerRuntime = createDefaultRuntime();

        secondRuntime = createDefaultRuntime();

        Predicate<Layout> twoNodeCheck = layout -> {
            boolean twoNodeCluster = layout.getAllServers().size() == 2;
            boolean oneSegment = layout.getSegments().size() == 1;

            return twoNodeCluster && oneSegment;
        };
        Layout currLayout = waitForLayoutChange(twoNodeCheck, firstRuntime);
        log.info("Two node cluster: {}", currLayout);

        // write records to the 2 node cluster
        final String data = createStringOfSize(100);
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            TokenResponse token = writerRuntime.getSequencerView().next();
            writerRuntime.getAddressSpaceView().write(token, data.getBytes());
        }

        firstRuntime
                .getAddressSpaceView()
                .commit(0, PARAMETERS.NUM_ITERATIONS_MODERATE - 1);

        // start a writer future
        final AtomicBoolean moreDataToBeWritten = new AtomicBoolean(true);
        CompletableFuture<Void> writerFuture = startWriter(moreDataToBeWritten);

        // use another thread to wait for layout change and fail node
        final Process killedServer = corfuServers.get(killNode);
        CompletableFuture<Void> killerFuture = CompletableFuture.runAsync(() -> {
            try {
                waitForLayoutChange(layout -> layout.getAllServers().size() == nodesCount, secondRuntime);
                assertThat(shutdownCorfuServer(killedServer)).isTrue();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // add node 9002
        firstRuntime
                .getManagementView()
                .addNode("localhost:9002", workflowNumRetry, timeout, pollPeriod);

        assertThatCode(killerFuture::join).doesNotThrowAnyException();

        // wait for killed node becomes unresponsive and state transfer completes
        Predicate<Layout> unresponsiveNodeCheck = layout -> {
            String killedNode = getServerEndpoint(basePort + killNode);

            boolean threeNodeCluster = layout.getAllServers().size() == nodesCount;
            boolean oneNodeUnresponsive = layout.getUnresponsiveServers().contains(killedNode);
            boolean oneSegment = layout.getSegments().size() == 1;

            return threeNodeCluster && oneNodeUnresponsive && oneSegment;
        };
        currLayout = waitForLayoutChange(unresponsiveNodeCheck, firstRuntime);
        log.info("Three node cluster with unresponsive layout: {}", currLayout);

        // bring killed node back and verify data
        Process resumedServer = runPersistentServer(corfuSingleNodeHost, basePort + killNode, false);
        Predicate<Layout> recoveredClusterCheck = layout -> {
            boolean threeNodeCluster = layout.getAllActiveServers().size() == nodesCount;
            boolean allNodesHealthy = layout.getUnresponsiveServers().isEmpty();
            boolean oneSegment = layout.getSegments().size() == 1;

            return threeNodeCluster && allNodesHealthy && oneSegment;
        };
        currLayout = waitForLayoutChange(recoveredClusterCheck, firstRuntime);
        log.info("Healthy cluster: {}", currLayout);

        moreDataToBeWritten.set(false);

        assertThatCode(writerFuture::join).doesNotThrowAnyException();

        verifyData(firstRuntime);

        shutdownCorfuServer(resumedServer);
        for (Process server : corfuServers) {
            shutdownCorfuServer(server);
        }
    }

    /**
     * A cluster of three nodes is started - 9000, 9001, 9002
     * Then a block of data of 15,000 entries is written to the cluster.
     * This is to ensure we have at least 1.5 data log files.
     * 1 node - 9002 is shutdown for a while, and triggers state transfer from two nodes.
     * Restart a node during transfer to verify it does not fail the transfer process.
     * Finally verify two rounds of transfer completes.
     *
     * @throws Exception error
     */
    private void verifyStateTransferWithNodeRestart(int restartNode) throws Exception {
        final int PORT_0 = 9000;
        final int PORT_1 = 9001;
        final int PORT_2 = 9002;
        final int nodesCount = 3;
        final int numEntries = 15_000;

        Process corfuServer_1 = runPersistentServer(corfuSingleNodeHost, PORT_0, false);
        Process corfuServer_2 = runPersistentServer(corfuSingleNodeHost, PORT_1, false);
        Process corfuServer_3 = runPersistentServer(corfuSingleNodeHost, PORT_2, false);

        // bootstrap cluster with 3 nodes
        final Layout twoNodeLayout = getLayout(3);
        BootstrapUtil.bootstrap(twoNodeLayout, retries, PARAMETERS.TIMEOUT_SHORT);

        firstRuntime = createDefaultRuntime();
        writerRuntime = createDefaultRuntime();
        secondRuntime = createDefaultRuntime();

        waitForLayoutChange(layout -> layout.getAllServers().size() == nodesCount
                && layout.getSegments().size() == 1, firstRuntime);

        // write 15,000 records to the 3 node cluster
        final String data = createStringOfSize(100);
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            TokenResponse token = writerRuntime.getSequencerView().next();
            writerRuntime.getAddressSpaceView().write(token, data.getBytes());
        }

        firstRuntime.getAddressSpaceView().commit(0, PARAMETERS.NUM_ITERATIONS_MODERATE - 1);

        // start a daemon writer
        final AtomicBoolean moreDataToBeWritten = new AtomicBoolean(true);
        CompletableFuture<Void> writerFuture = startWriter(moreDataToBeWritten);

        // shutdown node 9002 for a while, until it get marked as unresponsive
        assertThat(shutdownCorfuServer(corfuServer_3)).isTrue();
        waitForLayoutChange(layout -> layout.getAllServers().size() == nodesCount
                && layout.getUnresponsiveServers().contains(getServerEndpoint(PORT_2)), firstRuntime);

        // bring node 9002 back, it should trigger heal node workflow
        corfuServer_3 = runPersistentServer(corfuSingleNodeHost, PORT_2, false);

        // use another thread to wait for layout change and restart node
        CompletableFuture<Void> killerFuture = CompletableFuture.runAsync(() -> {
            try {
                waitForLayoutChange(layout -> layout.getAllServers().size() == nodesCount
                        && layout.getUnresponsiveServers().isEmpty(), secondRuntime);
                moreDataToBeWritten.set(false);
                restartServer(secondRuntime, getServerEndpoint(basePort + restartNode));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // wait for two round state transfer completes and verify data
        waitForLayoutChange(layout -> layout.getAllServers().size() == nodesCount
                && layout.getUnresponsiveServers().isEmpty()
                && layout.getSegments().size() == 1, firstRuntime);

        assertThatCode(killerFuture::join).doesNotThrowAnyException();
        assertThatCode(writerFuture::join).doesNotThrowAnyException();

        verifyData(firstRuntime);

        shutdownCorfuServer(corfuServer_1);
        shutdownCorfuServer(corfuServer_2);
        shutdownCorfuServer(corfuServer_3);
    }

    private CompletableFuture<Void> startWriter(AtomicBoolean stopFlag) {
        final int timeOut = 200;
        return CompletableFuture.runAsync(() -> {
            while (stopFlag.get()) {
                TokenResponse token = writerRuntime.getSequencerView().next();
                writerRuntime.getAddressSpaceView().write(token, "Test Payload".getBytes());
                Sleep.sleepUninterruptibly(Duration.ofMillis(timeOut));
            }
        });
    }

    /**
     * Queries for all the data in the 3 nodes and checks if the data state matches across the
     * cluster.
     *
     * @param corfuRuntime Connected instance of the runtime.
     * @throws Exception error
     */
    private void verifyData(CorfuRuntime corfuRuntime) throws Exception {
        long lastAddress = corfuRuntime.getSequencerView().query().getSequence();

        Map<Long, LogData> map0 = getAllNonEmptyData(corfuRuntime, "localhost:9000", lastAddress);
        Map<Long, LogData> map1 = getAllNonEmptyData(corfuRuntime, "localhost:9001", lastAddress);
        Map<Long, LogData> map2 = getAllNonEmptyData(corfuRuntime, "localhost:9002", lastAddress);

        List<Long> addresses = new ArrayList<>(map0.keySet());
        Collections.sort(addresses);

        for (Long addr : addresses) {
            if (!map1.containsKey(addr)) {
                fail("localhost:9001, missing address: " + addr);
            }

            if (!map2.containsKey(addr)) {
                fail("localhost:9002, missing address: " + addr);
            }

            LogData map0Data = map0.get(addr);
            LogData map1Data = map1.get(addr);
            LogData map2Data = map2.get(addr);

            String map1ErrMsg = "map0 != map1 log entry. map0: " + LogDataInfo.from(map0Data) +
                    ", map1: " + LogDataInfo.from(map1Data);
            assertEquals(map1ErrMsg, map0Data, map1Data);

            String map2ErrMsg = "map0 != map2 log entry. map0: " + LogDataInfo.from(map0Data) +
                    ", map2: " + LogDataInfo.from(map2Data);
            assertEquals(map2ErrMsg, map0Data, map2Data);
        }
    }

    @AllArgsConstructor
    @ToString
    private static class LogDataInfo {
        private final long globalAddress;
        private final DataType dataType;

        public static LogDataInfo from(LogData logData) {
            return new LogDataInfo(logData.getGlobalAddress(), logData.getType());
        }
    }

    /**
     * Fetches all the data from the node.
     *
     * @param corfuRuntime Connected instance of the runtime.
     * @param endpoint     Endpoint of query for all the data.
     * @param end          End address up to which data needs to be fetched.
     * @return Map of all the addresses contained by the node corresponding to the data stored.
     * @throws Exception error
     */
    private Map<Long, LogData> getAllNonEmptyData(CorfuRuntime corfuRuntime,
                                                  String endpoint, long end) throws Exception {
        ReadResponse readResponse = corfuRuntime.getLayoutView().getRuntimeLayout()
                .getLogUnitClient(endpoint)
                .read(getRangeAddressAsList(0L, end), false)
                .get();
        return readResponse.getAddresses().entrySet()
                .stream()
                .filter(longLogDataEntry -> !longLogDataEntry.getValue().isEmpty())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private List<Long> getRangeAddressAsList(long startAddress, long endAddress) {
        Range<Long> range = Range.closed(startAddress, endAddress);
        return ContiguousSet.create(range, DiscreteDomain.longs()).asList();
    }
}

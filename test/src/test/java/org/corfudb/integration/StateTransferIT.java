package org.corfudb.integration;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.google.common.reflect.TypeToken;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.Layout;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class StateTransferIT extends AbstractIT {

    private final String testStream = "test";
    private static String corfuSingleNodeHost;
    private final int basePort = 9000;
    private final int retries = 10;

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

    private Random getRandomNumberGenerator() {
        final Random randomSeed = new Random();
        final long SEED = randomSeed.nextLong();
        // Keep this print at all times to reproduce any failed test.
        testStatus += "SEED=" + Long.toHexString(SEED);
        return new Random(SEED);
    }

    @Before
    public void loadProperties() {
        corfuSingleNodeHost = (String) PROPERTIES.get("corfuSingleNodeHost");
    }

    @After
    public void tearDown() {
        if (runtime != null) {
            runtime.shutdown();
        }
    }

    @Test
    public void verifyStateTransferWithChainHeadFailure() throws Exception {
        verifyStateTransferWithNodeFailure(0);
    }

    @Test
    public void verifyStateTransferWithChainTailFailure() throws Exception {
        verifyStateTransferWithNodeFailure(1);
    }

    @Test
    public void verifyStateTransferWithChainHeadRestart() throws Exception {
        verifyStateTransferWithNodeRestart(0);
    }

    @Test
    public void verifyStateTransferWithChainTailRestart() throws Exception {
        verifyStateTransferWithNodeRestart(1);
    }


    /**
     * A cluster of two nodes is started - 9000, 9001.
     * Then a block of data entries is written to the cluster.
     * 1 node - 9002 is added to the cluster, and triggers parallel transfer from two nodes.
     * Fail a node during transfer to verify it does not fail the transfer process.
     * Finally the addition of node 9002 in the layout is verified.
     *
     * @throws Exception
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

        runtime = createDefaultRuntime();
        waitForLayoutChange(layout -> layout.getAllServers().size() == 2
                        && layout.getSegments().size() == 1,
                runtime);

        CorfuTable<String, String> table = runtime.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName(testStream)
                .open();

        // write records to the 2 node cluster
        final String data = createStringOfSize(100);
        Random r = getRandomNumberGenerator();
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            String key = Long.toString(r.nextLong());
            table.put(key, data);
        }

        // start a daemon writer
        final AtomicBoolean moreDataToBeWritten = new AtomicBoolean(true);
        Thread writer = startDaemonWriter(runtime, r, table, data, moreDataToBeWritten);

        // use another thread to wait for layout change and fail node
        final Process killedServer = corfuServers.get(killNode);
        Thread killer = new Thread(() -> {
            assertThatCode(() -> {
                waitForLayoutChange(layout -> layout.getAllServers().size() == nodesCount
                        && layout.getSegments().size() == 2, runtime);
                assertThat(shutdownCorfuServer(killedServer)).isTrue();
            }).doesNotThrowAnyException();
        });
        killer.setDaemon(true);
        killer.start();

        // add node 9002
        runtime.getManagementView().addNode("localhost:9002", workflowNumRetry,
                timeout, pollPeriod);
        killer.join();

        // wait for killed node becomes unresponsive and state transfer completes
        String killedNode = getServerEndpoint(basePort + killNode);
        waitForLayoutChange(layout -> layout.getAllServers().size() == nodesCount
                && layout.getUnresponsiveServers().contains(killedNode)
                && layout.getSegments().size() == 1, runtime);

        // bring killed node back and verify data
        Process resumedServer = runPersistentServer(corfuSingleNodeHost, basePort + killNode, false);
        waitForLayoutChange(layout -> layout.getAllActiveServers().size() == nodesCount
                && layout.getUnresponsiveServers().isEmpty()
                && layout.getSegments().size() == 1, runtime);

        moreDataToBeWritten.set(false);
        writer.join();
        verifyData(runtime);

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
     * @throws Exception
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

        runtime = createDefaultRuntime();
        waitForLayoutChange(layout -> layout.getAllServers().size() == nodesCount
                && layout.getSegments().size() == 1, runtime);

        CorfuTable<String, String> table = runtime.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName(testStream)
                .open();

        // write 15,000 records to the 3 node cluster
        final String data = createStringOfSize(100);
        Random r = getRandomNumberGenerator();
        for (int i = 0; i < numEntries; i++) {
            String key = Long.toString(r.nextLong());
            table.put(key, data);
        }

        // start a daemon writer
        final AtomicBoolean moreDataToBeWritten = new AtomicBoolean(true);
        Thread writer = startDaemonWriter(runtime, r, table, data, moreDataToBeWritten);

        // shutdown node 9002 for a while, until it get marked as unresponsive
        assertThat(shutdownCorfuServer(corfuServer_3)).isTrue();
        waitForLayoutChange(layout -> layout.getAllServers().size() == nodesCount
                && layout.getUnresponsiveServers().contains(getServerEndpoint(PORT_2)), runtime);

        // bring node 9002 back, it should trigger heal node workflow
        corfuServer_3 = runPersistentServer(corfuSingleNodeHost, PORT_2, false);

        // use another thread to wait for layout change and restart node
        Thread killer = new Thread(() -> {
            assertThatCode(() -> {
                waitForLayoutChange(layout -> layout.getAllServers().size() == nodesCount
                        && layout.getUnresponsiveServers().isEmpty(), runtime);
                moreDataToBeWritten.set(false);
                restartServer(runtime, getServerEndpoint(basePort + restartNode));
            }).doesNotThrowAnyException();
        });
        killer.setDaemon(true);
        killer.start();

        // wait for two round state transfer completes and verify data
        waitForLayoutChange(layout -> layout.getAllServers().size() == nodesCount
                && layout.getUnresponsiveServers().isEmpty()
                && layout.getSegments().size() == 1, runtime);

        killer.join();
        writer.join();
        verifyData(runtime);

        shutdownCorfuServer(corfuServer_1);
        shutdownCorfuServer(corfuServer_2);
        shutdownCorfuServer(corfuServer_3);
    }

    private Thread startDaemonWriter(CorfuRuntime corfuRuntime, Random r, CorfuTable table,
                                     String data, AtomicBoolean stopFlag) {
        Thread t = new Thread(() -> {
            while (stopFlag.get()) {
                assertThatCode(() -> {
                    try {
                        corfuRuntime.getObjectsView().TXBegin();
                        table.put(Integer.toString(r.nextInt()), data);
                        corfuRuntime.getObjectsView().TXEnd();
                    } catch (TransactionAbortedException e) {
                        // A transaction aborted exception is expected during
                        // some reconfiguration cases.
                        e.printStackTrace();
                    }
                }).doesNotThrowAnyException();
            }
        });
        t.setDaemon(true);
        t.start();
        return t;
    }

    /**
     * Queries for all the data in the 3 nodes and checks if the data state matches across the
     * cluster.
     *
     * @param corfuRuntime Connected instance of the runtime.
     * @throws Exception
     */
    private void verifyData(CorfuRuntime corfuRuntime) throws Exception {

        long lastAddress = corfuRuntime.getSequencerView().query(CorfuRuntime.getStreamID(testStream));

        Map<Long, LogData> map_0 = getAllNonEmptyData(corfuRuntime, "localhost:9000", lastAddress);
        Map<Long, LogData> map_1 = getAllNonEmptyData(corfuRuntime, "localhost:9001", lastAddress);
        Map<Long, LogData> map_2 = getAllNonEmptyData(corfuRuntime, "localhost:9002", lastAddress);

        assertThat(map_1.entrySet()).containsExactlyElementsOf(map_0.entrySet());
        assertThat(map_2.entrySet()).containsExactlyElementsOf(map_0.entrySet());
    }

    /**
     * Fetches all the data from the node.
     *
     * @param corfuRuntime Connected instance of the runtime.
     * @param endpoint     Endpoint of query for all the data.
     * @param end          End address up to which data needs to be fetched.
     * @return Map of all the addresses contained by the node corresponding to the data stored.
     * @throws Exception
     */
    private Map<Long, LogData> getAllNonEmptyData(CorfuRuntime corfuRuntime,
                                                  String endpoint, long end) throws Exception {
        ReadResponse readResponse = corfuRuntime.getLayoutView().getRuntimeLayout()
                .getLogUnitClient(endpoint)
                .readAll(getRangeAddressAsList(0L, end))
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

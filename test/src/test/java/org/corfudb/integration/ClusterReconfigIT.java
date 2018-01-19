package org.corfudb.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.google.common.collect.Range;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.Sleep;
import org.junit.Before;
import org.junit.Test;

public class ClusterReconfigIT extends AbstractIT {

    private static String corfuSingleNodeHost;

    @Before
    public void loadProperties() {
        corfuSingleNodeHost = (String) PROPERTIES.get("corfuSingleNodeHost");
    }

    private Random getRandomNumberGenerator() {
        final Random randomSeed = new Random();
        final long SEED = randomSeed.nextLong();
        // Keep this print at all times to reproduce any failed test.
        testStatus += "SEED=" + Long.toHexString(SEED);
        return new Random(SEED);
    }

    private Layout get3NodeLayout() throws Exception {
        return new Layout(
                new ArrayList<>(
                        Arrays.asList("localhost:9000", "localhost:9001", "localhost:9002")),
                new ArrayList<>(
                        Arrays.asList("localhost:9000", "localhost:9001", "localhost:9002")),
                Collections.singletonList(new Layout.LayoutSegment(
                        Layout.ReplicationMode.CHAIN_REPLICATION,
                        0L,
                        -1L,
                        Collections.singletonList(new Layout.LayoutStripe(
                                Arrays.asList("localhost:9000", "localhost:9001", "localhost:9002")
                        )))),
                0L,
                UUID.randomUUID());
    }

    /**
     * Creates a message of specified size in bytes.
     *
     * @param msgSize
     * @return
     */
    private static String createStringOfSize(int msgSize) {
        StringBuilder sb = new StringBuilder(msgSize);
        for (int i = 0; i < msgSize; i++) {
            sb.append('a');
        }
        return sb.toString();
    }

    private Process runSinglePersistentServer(String address, int port) throws IOException {
        return new CorfuServerRunner()
                .setHost(address)
                .setPort(port)
                .setLogPath(getCorfuServerLogPath(address, port))
                .setSingle(true)
                .runServer();
    }

    private Process runUnbootstrappedPersistentServer(String address, int port) throws IOException {
        return new CorfuServerRunner()
                .setHost(address)
                .setPort(port)
                .setLogPath(getCorfuServerLogPath(address, port))
                .setSingle(false)
                .runServer();
    }

    private Thread startDaemonWriter(CorfuRuntime runtime, Random r, CorfuTable table,
                                     String data, AtomicBoolean stopFlag) {
        Thread t = new Thread(() -> {
            while (stopFlag.get()) {
                assertThatCode(() -> {
                    try {
                        runtime.getObjectsView().TXBegin();
                        table.put(Integer.toString(r.nextInt()), data);
                        runtime.getObjectsView().TXEnd();
                    } catch (TransactionAbortedException e) {
                        // A transaction aborted exception is expected during
                        // some reconfiguration cases.
                    }
                }).doesNotThrowAnyException();
            }
        });
        t.setDaemon(true);
        t.start();
        return t;
    }

    /**
     * A cluster of one node is started - 9000.
     * Then a block of data of 15,000 entries is written to the node.
     * This is to ensure we have at least 1.5 data log files.
     * A daemon thread is instantiated to randomly put data while add node is executed.
     * 2 nodes - 9001 and 9002 are added to the cluster.
     * Finally the epoch and the addition of the 2 nodes in the laout is verified.
     *
     * @throws Exception
     */
    @Test
    public void addNodesTest() throws Exception {
        final int PORT_0 = 9000;
        final int PORT_1 = 9001;
        final int PORT_2 = 9002;
        final Duration timeout = Duration.ofMinutes(5);
        final Duration pollPeriod = Duration.ofSeconds(5);
        final int workflowNumRetry = 3;

        Process corfuServer_1 = runSinglePersistentServer(corfuSingleNodeHost, PORT_0);
        Process corfuServer_2 = runUnbootstrappedPersistentServer(corfuSingleNodeHost, PORT_1);
        Process corfuServer_3 = runUnbootstrappedPersistentServer(corfuSingleNodeHost, PORT_2);

        CorfuRuntime runtime = createDefaultRuntime();

        CorfuTable table = runtime.getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName("test")
                .open();

        final String data = createStringOfSize(1_000);

        Random r = getRandomNumberGenerator();
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            String key = Long.toString(r.nextLong());
            table.put(key, data);
        }

        final AtomicBoolean moreDataToBeWritten = new AtomicBoolean(true);
        Thread t = startDaemonWriter(runtime, r, table, data, moreDataToBeWritten);

        runtime.getManagementView().addNode("localhost:9001", workflowNumRetry,
                timeout, pollPeriod);
        runtime.getManagementView().addNode("localhost:9002", workflowNumRetry,
                timeout, pollPeriod);

        final long epochAfter2Adds = 4L;
        runtime.invalidateLayout();
        assertThat(runtime.getLayoutView().getLayout().getEpoch()).isEqualTo(epochAfter2Adds);

        moreDataToBeWritten.set(false);
        t.join();

        verifyData(runtime);

        shutdownCorfuServer(corfuServer_1);
        shutdownCorfuServer(corfuServer_2);
        shutdownCorfuServer(corfuServer_3);
    }

    /**
     * Queries for all the data in the 3 nodes and checks if the data state matches across the
     * cluster.
     *
     * @param corfuRuntime Connected instance of the runtime.
     * @throws Exception
     */
    private void verifyData(CorfuRuntime corfuRuntime) throws Exception {

        TokenResponse tokenResponse = corfuRuntime.getSequencerView()
                .nextToken(Collections.singleton(CorfuRuntime.getStreamID("test")), 0);
        long lastAddress = tokenResponse.getTokenValue();

        Map<Long, LogData> map_0 = getAllData(corfuRuntime, "localhost:9000", lastAddress);
        Map<Long, LogData> map_1 = getAllData(corfuRuntime, "localhost:9001", lastAddress);
        Map<Long, LogData> map_2 = getAllData(corfuRuntime, "localhost:9002", lastAddress);

        assertThat(map_1.equals(map_0)).isTrue();
        assertThat(map_2.equals(map_0)).isTrue();
    }

    /**
     * Fetches all the data from the node.
     *
     * @param corfuRuntime Connected instance of the runtime.
     * @param endpoint     Endpoint ot query for all the data.
     * @param end          End address up to which data needs to be fetched.
     * @return Map of all the addresses contained by the node corresponding to the data stored.
     * @throws Exception
     */
    private Map<Long, LogData> getAllData(CorfuRuntime corfuRuntime,
                                          String endpoint, long end) throws Exception {
        ReadResponse readResponse = corfuRuntime.getRouter(endpoint)
                .getClient(LogUnitClient.class)
                .read(Range.closed(0L, end))
                .get();
        return readResponse.getAddresses();
    }

    /**
     * Increments the epoch of the cluster by one. Keeps the remainder of the layout the same.
     *
     * @param corfuRuntime Connected runtime instance.
     * @return Returns the new accepted layout.
     * @throws Exception
     */
    private Layout incrementClusterEpoch(CorfuRuntime corfuRuntime) throws Exception {
        Layout l = new Layout(corfuRuntime.getLayoutView().getLayout());
        l.setRuntime(corfuRuntime);
        l.setEpoch(l.getEpoch() + 1);
        l.moveServersToEpoch();
        corfuRuntime.getLayoutView().updateLayout(l, 1L);
        corfuRuntime.invalidateLayout();
        assertThat(corfuRuntime.getLayoutView().getLayout().getEpoch()).isEqualTo(1L);
        return l;
    }

    /**
     * Starts a corfu server and increments the epoch from 0 to 1.
     * The server is then reset and the new layout fetch is expected to return a layout with
     * epoch 0 as all state should have been cleared.
     *
     * @throws Exception
     */
    @Test
    public void resetTest() throws Exception {

        final int PORT_0 = 9000;

        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, PORT_0);

        CorfuRuntime corfuRuntime = createDefaultRuntime();
        incrementClusterEpoch(corfuRuntime);
        corfuRuntime.getRouter("localhost:9000").getClient(BaseClient.class).reset().get();

        corfuRuntime = createDefaultRuntime();
        // The shutdown and reset can take an unknown amount of time and there is a chance that the
        // newer runtime may also connect to the older corfu server (before reset).
        // Hence the while loop.
        while (corfuRuntime.getLayoutView().getLayout().getEpoch() != 0L) {
            Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
            corfuRuntime = createDefaultRuntime();
        }
        assertThat(corfuRuntime.getLayoutView().getLayout().getEpoch()).isEqualTo(0L);
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Starts a corfu server and increments the epoch from 0 to 1.
     * The server is then restarted and the new layout fetch is expected to return a layout with
     * epoch 2 as the state is not cleared and the restart forces a recovery to increment the
     * epoch.
     *
     * @throws Exception
     */
    @Test
    public void restartTest() throws Exception {

        final int PORT_0 = 9000;

        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, PORT_0);

        CorfuRuntime corfuRuntime = createDefaultRuntime();
        Layout l = incrementClusterEpoch(corfuRuntime);
        corfuRuntime.getRouter("localhost:9000").getClient(BaseClient.class).restart()
                .get();

        corfuRuntime = createDefaultRuntime();
        // The shutdown and restart can take an unknown amount of time and there is a chance that
        // the newer runtime may also connect to the older corfu server (before restart).
        // Hence the while loop.
        while (corfuRuntime.getLayoutView().getLayout().getEpoch() != (l.getEpoch() + 1)) {
            Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
            corfuRuntime = createDefaultRuntime();
        }
        assertThat(corfuRuntime.getLayoutView().getLayout().getEpoch()).isEqualTo(l.getEpoch() + 1);
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Creates 3 corfu server processes.
     * Connects a runtime and creates a corfu table to write data.
     * One of the node is then shutdown/killed. The cluster then stabilizes to the new
     * epoch by removing the failure from the logunit chain and bootstrapping a backup
     * sequencer (only if required). After the stabilization is complete, another write is
     * attempted. The test passes only if this write goes through.
     *
     * @param killNode Index of the node to be killed.
     * @throws Exception
     */
    private void killNodeAndVerifyDataPath(int killNode)
            throws Exception {

        // Set up cluster of 3 nodes.
        // Set up cluster of 3 nodes.
        final int PORT_0 = 9000;
        final int PORT_1 = 9001;
        final int PORT_2 = 9002;
        Process corfuServer_1 = runUnbootstrappedPersistentServer(corfuSingleNodeHost, PORT_0);
        Process corfuServer_2 = runUnbootstrappedPersistentServer(corfuSingleNodeHost, PORT_1);
        Process corfuServer_3 = runUnbootstrappedPersistentServer(corfuSingleNodeHost, PORT_2);
        List<Process> corfuServers = Arrays.asList(corfuServer_1, corfuServer_2, corfuServer_3);
        Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
        final Layout layout = get3NodeLayout();
        bootstrapCluster(layout);

        // Create map and set up daemon writer thread.
        CorfuRuntime runtime = createDefaultRuntime();
        CorfuTable table = runtime.getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName("test")
                .open();
        final String data = createStringOfSize(1_000);
        Random r = getRandomNumberGenerator();
        final AtomicBoolean moreDataToBeWritten = new AtomicBoolean(true);
        Thread t = startDaemonWriter(runtime, r, table, data, moreDataToBeWritten);

        // Some preliminary writes into the corfu table.
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            runtime.getObjectsView().TXBegin();
            table.put(Integer.toString(r.nextInt()), data);
            runtime.getObjectsView().TXEnd();
        }

        // Killing killNode.
        assertThat(shutdownCorfuServer(corfuServers.get(killNode))).isTrue();

        // We wait for failure to be detected and the cluster to stabilize by waiting for the epoch
        // to increment.
        runtime.invalidateLayout();
        Layout refreshedLayout = runtime.getLayoutView().getLayout();
        while (refreshedLayout.getEpoch() == layout.getEpoch()) {
            runtime.invalidateLayout();
            refreshedLayout = runtime.getLayoutView().getLayout();
            Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
        }

        // Stop the daemon thread.
        moreDataToBeWritten.set(false);
        t.join();

        // Ensure writes still going through.
        // Fail test if write fails.
        boolean writeAfterKillNode = false;
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            try {
                runtime.getObjectsView().TXBegin();
                table.put(Integer.toString(r.nextInt()), data);
                runtime.getObjectsView().TXEnd();
                writeAfterKillNode = true;
                break;
            } catch (TransactionAbortedException tae) {
                // A transaction aborted exception is expected during
                // some reconfiguration cases.
                tae.printStackTrace();
            }
            Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
        }
        assertThat(writeAfterKillNode).isTrue();

        for (int i = 0; i < corfuServers.size(); i++) {
            if (i == killNode) {
                continue;
            }
            assertThat(shutdownCorfuServer(corfuServers.get(i))).isTrue();
        }
    }

    /**
     * Kill a node containing the head of the log unit chain and the primary sequencer.
     * Ensures writes go through after kill node and cluster re-stabilization.
     *
     * @throws Exception
     */
    @Test
    public void killLogUnitAndPrimarySequencer() throws Exception {
        killNodeAndVerifyDataPath(0);
    }

    /**
     * Kill a node containing one of the log unit chain and the non-primary sequencer.
     * Ensures writes go through after kill node and cluster re-stabilization.
     *
     * @throws Exception
     */
    @Test
    public void killLogUnitAndBackupSequencer() throws Exception {
        killNodeAndVerifyDataPath(1);
    }
}

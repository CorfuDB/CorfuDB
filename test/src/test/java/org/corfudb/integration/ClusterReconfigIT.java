package org.corfudb.integration;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;

import com.google.common.reflect.TypeToken;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.RebootUtil;
import org.corfudb.runtime.clients.BaseHandler;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LayoutHandler;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.clients.ManagementHandler;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.StreamsView;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.CFUtils;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.Sleep;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static junit.framework.TestCase.fail;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ClusterReconfigIT extends AbstractIT {

    private static String corfuSingleNodeHost;

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

    private String getServerEndpoint(int port) {
        return corfuSingleNodeHost + ":" + port;
    }

    private Random getRandomNumberGenerator() {
        final Random randomSeed = new Random();
        final long SEED = randomSeed.nextLong();
        // Keep this print at all times to reproduce any failed test.
        testStatus += "SEED=" + Long.toHexString(SEED);
        return new Random(SEED);
    }

    final int basePort = 9000;

    private Layout getLayout(int numNodes) {
        List<String> servers = new ArrayList<>();

        for (int x = 0; x < numNodes; x++) {
            String serverAddress = "localhost:" + (basePort + x);
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

    /**
     * Refreshes the layout and waits for a limited time for the refreshed layout to satisfy the
     * expected epoch verifier.
     *
     * @param epochVerifier Predicate to test the refreshed epoch value.
     * @param corfuRuntime  Runtime.
     */
    private void waitForEpochChange(Predicate<Long> epochVerifier, CorfuRuntime corfuRuntime)
            throws InterruptedException {

        corfuRuntime.invalidateLayout();
        Layout refreshedLayout = corfuRuntime.getLayoutView().getLayout();
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            if (epochVerifier.test(refreshedLayout.getEpoch())) {
                break;
            }
            corfuRuntime.invalidateLayout();
            refreshedLayout = corfuRuntime.getLayoutView().getLayout();
            TimeUnit.MILLISECONDS.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
        }
        assertThat(epochVerifier.test(refreshedLayout.getEpoch())).isTrue();
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

    private Process runPersistentServer(String address, int port, boolean singleNode) throws IOException {
        return new CorfuServerRunner()
                .setHost(address)
                .setPort(port)
                .setLogPath(getCorfuServerLogPath(address, port))
                .setSingle(singleNode)
                .runServer();
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
     * A cluster of one node is started - 9000.
     * Then a block of data of 15,000 entries is written to the node.
     * This is to ensure we have at least 1.5 data log files.
     * A daemon thread is instantiated to randomly put data while add node is executed.
     * 2 nodes - 9001 and 9002 are added to the cluster.
     * Finally the addition of the 2 nodes in the layout is verified.
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

        Process corfuServer_1 = runPersistentServer(corfuSingleNodeHost, PORT_0, true);
        Process corfuServer_2 = runPersistentServer(corfuSingleNodeHost, PORT_1, false);
        Process corfuServer_3 = runPersistentServer(corfuSingleNodeHost, PORT_2, false);

        runtime = createDefaultRuntime();

        CorfuTable<String, String> table = runtime.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
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

        final int nodesCount = 3;
        waitForLayoutChange(layout -> layout.getAllActiveServers().size() == nodesCount
                && layout.getUnresponsiveServers().isEmpty()
                && layout.getSegments().size() == 1, runtime);

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

        long lastAddress = corfuRuntime.getSequencerView().query(CorfuRuntime.getStreamID("test"));

        Map<Long, LogData> map_0 = getAllNonEmptyData(corfuRuntime, "localhost:9000", lastAddress);
        Map<Long, LogData> map_1 = getAllNonEmptyData(corfuRuntime, "localhost:9001", lastAddress);
        Map<Long, LogData> map_2 = getAllNonEmptyData(corfuRuntime, "localhost:9002", lastAddress);

        assertThat(map_1.entrySet()).containsOnlyElementsOf(map_0.entrySet());
        assertThat(map_2.entrySet()).containsOnlyElementsOf(map_0.entrySet());
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
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    /**
     * Increments the epoch of the cluster by one. Keeps the remainder of the layout the same.
     *
     * @param corfuRuntime Connected runtime instance.
     * @return Returns the new accepted layout.
     * @throws Exception
     */
    private Layout incrementClusterEpoch(CorfuRuntime corfuRuntime) throws Exception {
        long oldEpoch = corfuRuntime.getLayoutView().getLayout().getEpoch();
        Layout l = new Layout(corfuRuntime.getLayoutView().getLayout());
        l.setEpoch(l.getEpoch() + 1);
        corfuRuntime.getLayoutView().getRuntimeLayout(l).sealMinServerSet();
        corfuRuntime.getLayoutView().updateLayout(l, 1L);
        corfuRuntime.invalidateLayout();
        assertThat(corfuRuntime.getLayoutView().getLayout().getEpoch()).isEqualTo(oldEpoch + 1);
        return l;
    }

    private List<Long> getRangeAddressAsList(long startAddress, long endAddress) {
        Range<Long> range = Range.closed(startAddress, endAddress);
        return ContiguousSet.create(range, DiscreteDomain.longs()).asList();
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

        Process corfuServer = runPersistentServer(corfuSingleNodeHost, PORT_0, true);

        runtime = createDefaultRuntime();
        incrementClusterEpoch(runtime);
        runtime.getLayoutView().getRuntimeLayout().getBaseClient("localhost:9000")
                .reset().get();

        runtime = createDefaultRuntime();
        // The shutdown and reset can take an unknown amount of time and there is a chance that the
        // newer runtime may also connect to the older corfu server (before reset).
        // Hence the while loop.
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            if (runtime.getLayoutView().getLayout().getEpoch() == 0L) {
                break;
            }
            Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
            runtime = createDefaultRuntime();
        }
        assertThat(runtime.getLayoutView().getLayout().getEpoch()).isEqualTo(0L);
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Starts a corfu server and increments the epoch from 0 to 1.
     * The server is then restarted and the new layout fetch is expected to return a layout with
     * epoch 2 or greater (recovery can fail and epoch can increase) as the state is not cleared
     * and the restart forces a recovery to increment the epoch.
     *
     * @throws Exception
     */
    @Test
    public void restartTest() throws Exception {

        final int PORT_0 = 9000;

        Process corfuServer = runPersistentServer(corfuSingleNodeHost, PORT_0, true);

        runtime = createDefaultRuntime();
        Layout l = incrementClusterEpoch(runtime);
        runtime.getLayoutView().getRuntimeLayout(l).getBaseClient("localhost:9000")
                .restart().get();

        waitForEpochChange(epoch -> epoch >= l.getEpoch() + 1, runtime);

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Test reboot utility including reset and restart server
     * @throws Exception
     */
    @Test
    public void rebootUtilTest() throws Exception {
        final int PORT_0 = 9000;
        final String SERVER_0 = "localhost:" + PORT_0;
        final int retries = 3;

        // Start testing restart utility
        Process corfuServer = runPersistentServer(corfuSingleNodeHost, PORT_0, true);

        runtime = createDefaultRuntime();
        Layout layout = incrementClusterEpoch(runtime);
        RebootUtil.restart(SERVER_0, runtime.getParameters(), retries, PARAMETERS.TIMEOUT_LONG);

        waitForEpochChange(epoch -> epoch >= layout.getEpoch() + 1, runtime);

        runtime = createDefaultRuntime();
        RebootUtil.reset(SERVER_0, runtime.getParameters(), retries, PARAMETERS.TIMEOUT_LONG);
        runtime = createDefaultRuntime();

        waitForEpochChange(epoch -> epoch == 0, runtime);
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
    private void killNodeAndVerifyDataPath(int killNode) throws Exception {

        // Set up cluster of 3 nodes.
        final int PORT_0 = 9000;
        final int PORT_1 = 9001;
        final int PORT_2 = 9002;
        Process corfuServer_1 = runPersistentServer(corfuSingleNodeHost, PORT_0, false);
        Process corfuServer_2 = runPersistentServer(corfuSingleNodeHost, PORT_1, false);
        Process corfuServer_3 = runPersistentServer(corfuSingleNodeHost, PORT_2, false);
        List<Process> corfuServers = Arrays.asList(corfuServer_1, corfuServer_2, corfuServer_3);
        final Layout layout = getLayout(3);
        final int retries = 3;
        TimeUnit.SECONDS.sleep(1);
        BootstrapUtil.bootstrap(layout, retries, PARAMETERS.TIMEOUT_SHORT);

        // Create map and set up daemon writer thread.
        runtime = createDefaultRuntime();
        CorfuTable<String, String> table = runtime.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
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
        waitForEpochChange(refreshedEpoch -> refreshedEpoch > layout.getEpoch(), runtime);

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

    /**
     * Bootstrap cluster of 3 nodes using the BootstrapUtil.
     *
     * @throws Exception
     */
    @Test
    public void test3NodeBootstrapUtility() throws Exception {

        // Set up cluster of 3 nodes.
        final int PORT_0 = 9000;
        final int PORT_1 = 9001;
        final int PORT_2 = 9002;
        Process corfuServer_1 = runPersistentServer(corfuSingleNodeHost, PORT_0, false);
        Process corfuServer_2 = runPersistentServer(corfuSingleNodeHost, PORT_1, false);
        Process corfuServer_3 = runPersistentServer(corfuSingleNodeHost, PORT_2, false);
        final Layout layout = getLayout(3);

        final int retries = 3;
        BootstrapUtil.bootstrap(layout, retries, PARAMETERS.TIMEOUT_SHORT);

        runtime = createDefaultRuntime();
        assertThat(runtime.getLayoutView().getLayout().equals(layout)).isTrue();

        shutdownCorfuServer(corfuServer_1);
        shutdownCorfuServer(corfuServer_2);
        shutdownCorfuServer(corfuServer_3);
    }

    private void retryBootstrapOperation(Runnable bootstrapOperation) throws InterruptedException {
        final int retries = 5;
        int retry = retries;
        while (retry-- > 0) {
            try {
                bootstrapOperation.run();
                return;
            } catch (Exception e) {
                TimeUnit.MILLISECONDS.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
            }
        }
        fail();
    }

    /**
     * Setup a cluster of 1 node. Bootstrap the nodes.
     * Bootstrap the cluster using the BootstrapUtil. It should assert that the node already
     * bootstrapped is with the same layout and then bootstrap the cluster.
     */
    @Test
    public void test1NodeBootstrapWithBootstrappedNode() throws Exception {

        // Set up cluster of 1 nodes.
        final int PORT_0 = 9000;
        Process corfuServer_1 = runPersistentServer(corfuSingleNodeHost, PORT_0, false);
        final Layout layout = getLayout(1);
        final int retries = 5;

        IClientRouter router = new NettyClientRouter(NodeLocator
                .parseString(corfuSingleNodeHost + ":" + PORT_0),
                CorfuRuntime.CorfuRuntimeParameters.builder().build());
        router.addClient(new LayoutHandler()).addClient(new BaseHandler());
        retryBootstrapOperation(() -> CFUtils.getUninterruptibly(
                new LayoutClient(router, layout.getEpoch()).bootstrapLayout(layout)));
        retryBootstrapOperation(() -> CFUtils.getUninterruptibly(
                new ManagementClient(router, layout.getEpoch()).bootstrapManagement(layout)));

        BootstrapUtil.bootstrap(layout, retries, PARAMETERS.TIMEOUT_SHORT);

        runtime = createDefaultRuntime();
        assertThat(runtime.getLayoutView().getLayout().equals(layout)).isTrue();

        shutdownCorfuServer(corfuServer_1);
    }

    /**
     * Setup a cluster of 1 node. Bootstrap the node with a wrong layout.
     * Bootstrap the cluster using the BootstrapUtil. It should assert that the node already
     * bootstrapped is with the wrong layout and then fail with the AlreadyBootstrappedException.
     */
    @Test
    public void test1NodeBootstrapWithWrongBootstrappedLayoutServer() throws Exception {

        // Set up cluster of 1 node.
        final int PORT_0 = 9000;
        Process corfuServer_1 = runPersistentServer(corfuSingleNodeHost, PORT_0, false);
        final Layout layout = getLayout(1);
        final int retries = 3;

        IClientRouter router = new NettyClientRouter(NodeLocator
                .parseString(corfuSingleNodeHost + ":" + PORT_0),
                CorfuRuntime.CorfuRuntimeParameters.builder().build());
        router.addClient(new LayoutHandler()).addClient(new BaseHandler());
        Layout wrongLayout = new Layout(layout);
        wrongLayout.getLayoutServers().add("localhost:9005");
        retryBootstrapOperation(() -> CFUtils.getUninterruptibly(
                new LayoutClient(router, layout.getEpoch()).bootstrapLayout(wrongLayout)));

        assertThatThrownBy(() -> BootstrapUtil.bootstrap(layout, retries, PARAMETERS.TIMEOUT_SHORT))
                .hasCauseInstanceOf(AlreadyBootstrappedException.class);

        shutdownCorfuServer(corfuServer_1);
        router.stop();
    }

    /**
     * Setup a cluster of 1 node. Bootstrap the node with a wrong layout.
     * Bootstrap the cluster using the BootstrapUtil. It should assert that the node already
     * bootstrapped is with the wrong layout and then fail with the AlreadyBootstrappedException.
     */
    @Test
    public void test1NodeBootstrapWithWrongBootstrappedManagementServer() throws Exception {

        // Set up cluster of 1 node.
        final int PORT_0 = 9000;
        Process corfuServer_1 = runPersistentServer(corfuSingleNodeHost, PORT_0, false);
        final Layout layout = getLayout(1);
        final int retries = 3;

        IClientRouter router = new NettyClientRouter(NodeLocator
                .parseString(corfuSingleNodeHost + ":" + PORT_0),
                CorfuRuntime.CorfuRuntimeParameters.builder().build());
        router.addClient(new LayoutHandler())
                .addClient(new ManagementHandler())
                .addClient(new BaseHandler());
        Layout wrongLayout = new Layout(layout);
        final ManagementClient managementClient = new ManagementClient(router, layout.getEpoch());
        wrongLayout.getLayoutServers().add("localhost:9005");
        retryBootstrapOperation(() -> CFUtils.getUninterruptibly(
                managementClient.bootstrapManagement(wrongLayout)));

        assertThatThrownBy(() -> BootstrapUtil.bootstrap(layout, retries, PARAMETERS.TIMEOUT_SHORT))
                .hasCauseInstanceOf(AlreadyBootstrappedException.class);

        shutdownCorfuServer(corfuServer_1);
        router.stop();
    }


    /**
     * Starts with 3 nodes on ports 0, 1 and 2.
     * A daemon thread is started for random writes in the background.
     * PART 1. Kill node.
     * First the node on port 0 is killed. The test then waits for the cluster to stabilize and
     * assert that data operations still go through.
     * Part 2. Revive node.
     * Now, the same node is revived and waits for the node to be healed and merged back into the
     * cluster.
     * Finally the data on all the 3 nodes is verified.
     *
     * @throws Exception
     */
    @Test
    public void killAndHealNode() throws Exception {

        // Set up cluster of 3 nodes.
        final int PORT_0 = 9000;
        final int PORT_1 = 9001;
        final int PORT_2 = 9002;
        Process corfuServer_1 = runPersistentServer(corfuSingleNodeHost, PORT_0, false);
        Process corfuServer_2 = runPersistentServer(corfuSingleNodeHost, PORT_1, false);
        Process corfuServer_3 = runPersistentServer(corfuSingleNodeHost, PORT_2, false);
        final Layout layout = getLayout(3);
        final int retries = 3;
        BootstrapUtil.bootstrap(layout, retries, PARAMETERS.TIMEOUT_SHORT);

        // Create map and set up daemon writer thread.
        runtime = createDefaultRuntime();
        CorfuTable<String, String> table = runtime.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName("test")
                .open();
        final String data = createStringOfSize(1_000);
        Random r = getRandomNumberGenerator();

        // Some preliminary writes into the corfu table.
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            runtime.getObjectsView().TXBegin();
            table.put(Integer.toString(r.nextInt()), data);
            runtime.getObjectsView().TXEnd();
        }

        // PART 1.
        // Killing killNode.
        assertThat(shutdownCorfuServer(corfuServer_1)).isTrue();

        // We wait for failure to be detected and the cluster to stabilize by waiting for the epoch
        // to increment.
        waitFor(() -> {
            runtime.invalidateLayout();
            Layout refreshedLayout = runtime.getLayoutView().getLayout();
            return refreshedLayout.getAllActiveServers().size() == 2
                    && refreshedLayout.getUnresponsiveServers().contains("localhost:9000");
        });

        // Ensure writes still going through. Daemon writer thread does not ensure this.
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
            }
            TimeUnit.MILLISECONDS.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
        }
        assertThat(writeAfterKillNode).isTrue();

        // PART 2.
        // Reviving same node.
        corfuServer_1 = runPersistentServer(corfuSingleNodeHost, PORT_0, false);

        // Waiting node to be healed and added back to the layout.
        waitFor(() -> {
            runtime.invalidateLayout();
            Layout refreshedLayout = runtime.getLayoutView().getLayout();
            return refreshedLayout.getSegments().size() == 1
                    && refreshedLayout.getUnresponsiveServers().size() == 0
                    && refreshedLayout.getAllActiveServers().size() == layout.getAllServers().size();
        });
        verifyData(runtime);

        shutdownCorfuServer(corfuServer_1);
        shutdownCorfuServer(corfuServer_2);
        shutdownCorfuServer(corfuServer_3);
    }

    /**
     * This test starts a single corfu node. The CorfuRuntime is registered with a
     * systemDownHandler which releases a semaphore. The test waits on this semaphore to
     * complete successfully.
     */
    @Test
    public void testSystemDownHandler() throws Exception {

        final int PORT_0 = 9000;
        Process corfuServer_1 = runPersistentServer(corfuSingleNodeHost, PORT_0, true);
        final Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        // Create map and set up daemon writer thread.
        CorfuRuntimeParameters corfuRuntimeParameters = CorfuRuntimeParameters.builder()
                .layoutServer(NodeLocator.parseString("localhost:9000"))
                .cacheDisabled(true)
                .systemDownHandlerTriggerLimit(1)
                // Register the system down handler to throw a RuntimeException.
                .systemDownHandler(() ->
                        assertThatCode(semaphore::release).doesNotThrowAnyException())
                .build();

        runtime = CorfuRuntime.fromParameters(corfuRuntimeParameters).connect();

        CorfuTable<String, String> table = runtime.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName("test")
                .open();
        final String data = createStringOfSize(1_000);
        Random r = getRandomNumberGenerator();

        // A preliminary write into the corfu table.
        runtime.getObjectsView().TXBegin();
        table.put(Integer.toString(r.nextInt()), data);
        runtime.getObjectsView().TXEnd();

        // Killing the server.
        assertThat(shutdownCorfuServer(corfuServer_1)).isTrue();

        Thread t = new Thread(() -> {
            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
                try {
                    runtime.getObjectsView().TXBegin();
                    table.put(Integer.toString(r.nextInt()), data);
                    runtime.getObjectsView().TXEnd();
                    break;
                } catch (TransactionAbortedException tae) {
                    // A transaction aborted exception is expected during
                    // some reconfiguration cases.
                }
            }
        });
        t.start();

        // Wait for the systemDownHandler to be invoked.
        semaphore.tryAcquire(PARAMETERS.TIMEOUT_LONG.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Ensure that multiple resets on the same epoch reset the log unit server only once.
     * Consider 3 nodes 9000, 9001 and 9002.
     * Kill node 9002 and recover it so that the heal node workflow is triggered, the node is
     * reset and added back to the chain.
     * We then perform a write on addresses 6-10.
     * Now we attempt to reset the node 9002 again on the same epoch and assert that the write
     * performed on address 1 is not erased by the new reset.
     */
    @Test
    public void preventMultipleResets() throws Exception {
        // Set up cluster of 3 nodes.
        final int PORT_0 = 9000;
        final int PORT_1 = 9001;
        final int PORT_2 = 9002;
        Process corfuServer_1 = runPersistentServer(corfuSingleNodeHost, PORT_0, false);
        Process corfuServer_2 = runPersistentServer(corfuSingleNodeHost, PORT_1, false);
        Process corfuServer_3 = runPersistentServer(corfuSingleNodeHost, PORT_2, false);
        final Layout layout = getLayout(3);

        final int retries = 3;
        BootstrapUtil.bootstrap(layout, retries, PARAMETERS.TIMEOUT_SHORT);

        runtime = createDefaultRuntime();
        UUID streamId = CorfuRuntime.getStreamID("testStream");
        IStreamView stream = runtime.getStreamsView().get(streamId);
        int counter = 0;

        // Shutdown server 3.
        shutdownCorfuServer(corfuServer_3);
        waitForEpochChange(refreshedEpoch -> refreshedEpoch > layout.getEpoch(), runtime);
        final Layout layoutAfterFailure = runtime.getLayoutView().getLayout();

        final int appendNum = 5;
        // Write at address 0-4.
        for (int i = 0; i < appendNum; i++) {
            stream.append(Integer.toString(counter++).getBytes());
        }

        // Restart server 3 and wait for heal node workflow to modify the layout.
        corfuServer_3 = runPersistentServer(corfuSingleNodeHost, PORT_2, false);
        waitForEpochChange(refreshedEpoch -> refreshedEpoch > layoutAfterFailure.getEpoch(), runtime);

        // Write at address 5-9.
        final long startAddress = 0L;
        final long endAddress = 9L;
        for (int i = 0; i < appendNum; i++) {
            stream.append(Integer.toString(counter++).getBytes());
        }

        // Trigger a reset on the node.
        runtime.getLayoutView().getRuntimeLayout(layoutAfterFailure)
                .getLogUnitClient("localhost:9002")
                .resetLogUnit(layoutAfterFailure.getEpoch()).get();

        // Verify data
        int verificationCounter = 0;
        for (LogData logData : runtime.getLayoutView().getRuntimeLayout()
                .getLogUnitClient("localhost:9002")
                .readAll(getRangeAddressAsList(startAddress, endAddress))
                .get().getAddresses().values()) {
            assertThat(logData.getPayload(runtime))
                    .isEqualTo(Integer.toString(verificationCounter++).getBytes());
        }

        shutdownCorfuServer(corfuServer_1);
        shutdownCorfuServer(corfuServer_2);
        shutdownCorfuServer(corfuServer_3);
    }

    /**
     * Test that a node with a stale layout can recover.
     * Consider a cluster of 3 nodes - 9000, 9001 and 9002.
     * 9002 is first shutdown. This node is now stuck with the layout at epoch 0.
     * Now 9000 is restarted, forcing an epoch increment.
     * Next, 9002 is also restarted. The test then asserts that even though 9002 is lagging
     * by a few epochs, it recovers and is added back to the chain.
     */
    @Test
    public void healNodesWorkflowTest() throws Exception {
        // Set up cluster of 3 nodes.
        final int PORT_0 = 9000;
        final int PORT_1 = 9001;
        final int PORT_2 = 9002;
        Process corfuServer_1 = runPersistentServer(corfuSingleNodeHost, PORT_0, false);
        Process corfuServer_2 = runPersistentServer(corfuSingleNodeHost, PORT_1, false);
        Process corfuServer_3 = runPersistentServer(corfuSingleNodeHost, PORT_2, false);
        final int nodesCount = 3;
        final Layout layout = getLayout(3);

        final int retries = 3;
        BootstrapUtil.bootstrap(layout, retries, PARAMETERS.TIMEOUT_SHORT);

        runtime = createDefaultRuntime();
        UUID streamId = CorfuRuntime.getStreamID("testStream");
        IStreamView stream = runtime.getStreamsView().get(streamId);

        shutdownCorfuServer(corfuServer_3);
        waitForEpochChange(refreshedEpoch -> refreshedEpoch > layout.getEpoch(), runtime);
        final Layout layoutAfterFailure1 = runtime.getLayoutView().getLayout();

        shutdownCorfuServer(corfuServer_1);
        corfuServer_1 = runPersistentServer(corfuSingleNodeHost, PORT_0, false);
        waitForEpochChange(refreshedEpoch -> refreshedEpoch > layoutAfterFailure1.getEpoch(), runtime);
        final Layout layoutAfterHeal1 = runtime.getLayoutView().getLayout();
        int counter = 0;

        final int appendNum = 3;
        final long startAddress = 0;
        // Write at address 0-1-2
        for (int i = 0; i < appendNum; i++) {
            stream.append(Integer.toString(counter++).getBytes());
        }

        corfuServer_3 = runPersistentServer(corfuSingleNodeHost, PORT_2, false);
        waitForEpochChange(refreshedEpoch -> refreshedEpoch > layoutAfterHeal1.getEpoch(), runtime);

        // Write at address 3-4-5
        for (int i = 0; i < appendNum; i++) {
            stream.append(Integer.toString(counter++).getBytes());
        }
        final long endAddress = 5;

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            Layout finalLayout = runtime.getLayoutView().getLayout();
            if (finalLayout.getUnresponsiveServers().isEmpty()
                    && finalLayout.getSegments().size() == 1
                    && finalLayout.getSegments().get(0).getAllLogServers().size() == nodesCount) {
                break;
            }
            runtime.invalidateLayout();
            TimeUnit.MILLISECONDS.sleep(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        }

        assertThat(runtime.getLayoutView().getLayout().getUnresponsiveServers()).isEmpty();

        // Verify data
        int verificationCounter = 0;
        for (LogData logData : runtime.getLayoutView().getRuntimeLayout()
                .getLogUnitClient("localhost:9002")
                .readAll(getRangeAddressAsList(startAddress, endAddress)).get()
                .getAddresses().values()) {
            assertThat(logData.getPayload(runtime))
                    .isEqualTo(Integer.toString(verificationCounter++).getBytes());
        }

        shutdownCorfuServer(corfuServer_1);
        shutdownCorfuServer(corfuServer_2);
        shutdownCorfuServer(corfuServer_3);
    }

    /**
     * Test that a disconnected CorfuRuntime using a systemDownHandler reconnects itself once the
     * cluster is back online. The client disconnection is simulated by taking 2 servers
     * 9000 and 9002 offline by shutting them down. They are then restarted and the date is
     * validated.
     */
    @Test
    public void reconnectDisconnectedClient() throws Exception {
        // Set up cluster of 3 nodes.
        final int PORT_0 = 9000;
        final int PORT_1 = 9001;
        final int PORT_2 = 9002;
        Process corfuServer_1 = runPersistentServer(corfuSingleNodeHost, PORT_0, false);
        Process corfuServer_2 = runPersistentServer(corfuSingleNodeHost, PORT_1, false);
        Process corfuServer_3 = runPersistentServer(corfuSingleNodeHost, PORT_2, false);
        final Layout layout = getLayout(3);

        final int retries = 3;
        BootstrapUtil.bootstrap(layout, retries, PARAMETERS.TIMEOUT_SHORT);

        final int systemDownHandlerLimit = 10;
        runtime = CorfuRuntime.fromParameters(CorfuRuntimeParameters.builder()
                .layoutServer(NodeLocator.parseString(DEFAULT_ENDPOINT))
                .systemDownHandlerTriggerLimit(systemDownHandlerLimit)
                // Register the system down handler to throw a RuntimeException.
                .systemDownHandler(() -> {
                    throw new RuntimeException("SystemDownHandler");
                })
                .build()).connect();

        UUID streamId = CorfuRuntime.getStreamID("testStream");
        IStreamView stream = runtime.getStreamsView().get(streamId);

        final int appendNum = 3;
        int counter = 0;
        // Preliminary writes.
        for (int i = 0; i < appendNum; i++) {
            stream.append(Integer.toString(counter++).getBytes());
        }

        // Shutdown the servers 9000 and 9002.
        shutdownCorfuServer(corfuServer_1);
        shutdownCorfuServer(corfuServer_3);

        final Semaphore latch = new Semaphore(1);
        latch.acquire();

        // Invalidate the layout so that there are no cached layouts.
        runtime.invalidateLayout();

        // Spawn a thread which tries to append to the stream. This should initially get stuck and
        // trigger the systemDownHandler.
        // This would start the server and finally wait for the append to complete.
        Thread t = new Thread(() -> {
            final int counterValue = 3;
            while (true) {
                try {
                    stream.append(Integer.toString(counterValue).getBytes());
                    break;
                } catch (RuntimeException sue) {
                    // Release latch and try again..
                    latch.release();
                }
            }
        });
        t.start();

        assertThat(latch.tryAcquire(PARAMETERS.TIMEOUT_LONG.toMillis(), TimeUnit.MILLISECONDS))
                .isTrue();
        // Restart both the servers.
        corfuServer_1 = runPersistentServer(corfuSingleNodeHost, PORT_0, false);
        corfuServer_3 = runPersistentServer(corfuSingleNodeHost, PORT_2, false);

        t.join(PARAMETERS.TIMEOUT_LONG.toMillis());

        // Verify data
        final int startAddress = 0;
        final int endAddress = 3;
        for (int i = startAddress; i <= endAddress; i++) {
            assertThat(runtime.getAddressSpaceView().read(i).getPayload(runtime))
                    .isEqualTo(Integer.toString(i).getBytes());
        }

        shutdownCorfuServer(corfuServer_1);
        shutdownCorfuServer(corfuServer_2);
        shutdownCorfuServer(corfuServer_3);
    }

    /**
     * Tests a force remove after 2 nodes have been shutdown and the remaining one node
     * has sealed itself.
     * Setup: 3 node cluster.
     * 2 nodes PORT_1 and PORT_2 are shutdown.
     * A runtime attempts to write data into the cluster with a registered systemDownHandler.
     * Once the systemDownHandler is invoked, we can be assured that the 2 nodes are shutdown.
     * We verify that the remaining one node PORT_0 has sealed itself by sending a
     * NodeStatusRequest. This is responded by a WrongEpochException.
     * Finally we attempt to force remove the 2 shutdown nodes.
     */
    @Test
    public void forceRemoveAfterSeal() throws Exception {

        // Set up cluster of 3 nodes.
        final int PORT_0 = 9000;
        final int PORT_1 = 9001;
        final int PORT_2 = 9002;
        Process corfuServer_1 = runPersistentServer(corfuSingleNodeHost, PORT_0, false);
        Process corfuServer_2 = runPersistentServer(corfuSingleNodeHost, PORT_1, false);
        Process corfuServer_3 = runPersistentServer(corfuSingleNodeHost, PORT_2, false);
        List<Process> corfuServers = Arrays.asList(corfuServer_1, corfuServer_2, corfuServer_3);
        final Layout layout = getLayout(3);
        final int retries = 3;
        TimeUnit.SECONDS.sleep(1);
        BootstrapUtil.bootstrap(layout, retries, PARAMETERS.TIMEOUT_SHORT);

        final int systemDownHandlerLimit = 10;
        runtime = CorfuRuntime.fromParameters(CorfuRuntimeParameters.builder()
                .layoutServer(NodeLocator.parseString(DEFAULT_ENDPOINT))
                .systemDownHandler(() -> {
                    throw new RuntimeException();
                })
                .systemDownHandlerTriggerLimit(systemDownHandlerLimit)
                .build())
                .connect();
        IStreamView stream = runtime.getStreamsView().get(CorfuRuntime.getStreamID("testStream"));

        shutdownCorfuServer(corfuServer_2);
        shutdownCorfuServer(corfuServer_3);

        // Wait for systemDownHandler to be invoked.
        while (true) {
            try {
                stream.append("test".getBytes());
                Sleep.sleepUninterruptibly(PARAMETERS.TIMEOUT_VERY_SHORT);
            } catch (RuntimeException re) {
                break;
            }
        }

        // Assert that the live node has been sealed.
        assertThatThrownBy(
                () -> CFUtils.getUninterruptibly(runtime.getLayoutView().getRuntimeLayout()
                        .getManagementClient(corfuSingleNodeHost + ":" + PORT_0)
                        .sendNodeStateRequest(), WrongEpochException.class))
                .isInstanceOf(WrongEpochException.class);

        runtime.getManagementView().forceRemoveNode(
                corfuSingleNodeHost + ":" + PORT_1,
                retries,
                PARAMETERS.TIMEOUT_SHORT,
                PARAMETERS.TIMEOUT_LONG);
        runtime.getManagementView().forceRemoveNode(
                corfuSingleNodeHost + ":" + PORT_2,
                retries,
                PARAMETERS.TIMEOUT_SHORT,
                PARAMETERS.TIMEOUT_LONG);
        runtime.invalidateLayout();

        // Assert that there is only one node in the layout.
        assertThat(runtime.getLayoutView().getLayout().getAllServers())
                .containsExactly(corfuSingleNodeHost + ":" + PORT_0);

        for (Process corfuServer : corfuServers) {
            shutdownCorfuServer(corfuServer);
        }
    }

    @Test
    public void testFailoverWithStreamSubsumedByCheckpointNoTrim() throws Exception {
        // Verify that despite no trim has been performed and stream trim mark is set on state transfer (while
        // addresses are still present in the address map). Streams are correctly rebuilt.
        testFailoverWithStreamSubsumedByCheckpoint(false);
    }

    @Test
    public void testFailoverWithStreamSubsumedByCheckpointWithTrim() throws Exception {
        testFailoverWithStreamSubsumedByCheckpoint(true);
    }

    /**
     * Verify that a stream can be correctly loaded after a reconfiguration and
     * all the updates on this stream are subsumed by a checkpoint (trimmed and not).
     */
    private void testFailoverWithStreamSubsumedByCheckpoint(boolean trim) throws Exception {
        final int PORT_0 = 9000;
        final int PORT_1 = 9001;
        final int PORT_2 = 9002;

        final String streamName = "stream1";

        final int numRetry = 3;
        final Duration timeout = Duration.ofMinutes(5);
        final Duration pollPeriod = Duration.ofSeconds(5);

        final UUID streamId = CorfuRuntime.getStreamID(streamName);
        final String checkpointStreamName = CorfuRuntime.getStreamID(streamName).toString()
                + StreamsView.CHECKPOINT_SUFFIX;
        final UUID checkpointStreamId = UUID.nameUUIDFromBytes(checkpointStreamName.getBytes());

        final int numCheckpointRecordsDefault = 3;

        Process server0 = runPersistentServer(corfuSingleNodeHost, PORT_0, true);
        runtime = createDefaultRuntime();

        CorfuTable<String, String> table = runtime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamID(streamId)
                .open();

        final int numEntries = 3;
        for (int i = 0; i < numEntries; i++) {
            table.put("k" + i, "v" + i);
        }

        // Checkpoint and trim the stream, no further updates on this stream (subsumed by checkpoint).
        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(table);
        Token token = mcw.appendCheckpoints(runtime, "author");
        if (trim) {
            runtime.getAddressSpaceView().prefixTrim(token);
        }

        // Add two other servers that triggers state transfer.
        Process server1 = runPersistentServer(corfuSingleNodeHost, PORT_1, false);
        runtime.getManagementView().addNode(getServerEndpoint(PORT_1), numRetry, timeout, pollPeriod);

        Process server2 = runPersistentServer(corfuSingleNodeHost, PORT_2, false);
        runtime.getManagementView().addNode(getServerEndpoint(PORT_2), numRetry, timeout, pollPeriod);

        runtime.invalidateLayout();
        Layout layout = runtime.getLayoutView().getLayout();

        // Kill the server with primary sequencer, which triggers sequencer failover.
        assertThat(shutdownCorfuServer(server0)).isTrue();
        waitForEpochChange(refreshedEpoch -> refreshedEpoch > layout.getEpoch(), runtime);

        // Create a new runtime.
        CorfuRuntime runtime2 = createRuntime(getServerEndpoint(PORT_1));

        // Verify sequencer has the correct steam tail.
        assertThat(runtime2.getLayoutView().getLayout().getPrimarySequencer()).isNotEqualTo(getServerEndpoint(PORT_0));
        assertThat(runtime2.getSequencerView().query(streamId)).isEqualTo(numEntries);

        CorfuTable<String, String> table2 = runtime2.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamID(streamId)
                .open();

        // Verify sequencer has correct address map for this stream (addresses and trim mark)
        StreamAddressSpace addressSpace = runtime2.getAddressSpaceView()
                .getLogAddressSpace()
                .getAddressMap().get(streamId);

        assertThat(addressSpace.getTrimMark()).isEqualTo(numEntries);
        if (trim) {
            // Addresses were trimmed, cardinality of addresses should be 0
            assertThat(addressSpace.getAddressMap().getLongCardinality()).isEqualTo(0L);
        } else {
            // Extra entry corresponds to entry added by checkpointer.
            assertThat(addressSpace.getAddressMap().getLongCardinality()).isEqualTo(numEntries+1);
        }

        // Verify START_ADDRESS of checkpoint for stream
        StreamAddressSpace checkpointAddressSpace = runtime2.getAddressSpaceView()
                .getLogAddressSpace()
                .getAddressMap().get(checkpointStreamId);

        // Addresses should correspond to: start, continuation and end records. (total 3 records)
        assertThat(checkpointAddressSpace.getAddressMap().getLongCardinality()).isEqualTo(numCheckpointRecordsDefault);
        CheckpointEntry cpEntry = (CheckpointEntry) runtime2.getAddressSpaceView()
                .read(checkpointAddressSpace.getHighestAddress())
                .getPayload(runtime2);
        assertThat(cpEntry.getDict().get(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS)).
                isEqualTo(String.valueOf(numEntries));

        // Verify the object has correct state.
        assertThat(table2.size()).isEqualTo(numEntries);
        for (int i = 0; i < numEntries; i++) {
            assertThat(table2.get("k" + i)).isEqualTo("v" + i);
        }

        shutdownCorfuServer(server1);
        shutdownCorfuServer(server2);
    }
}

package org.corfudb.integration;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;
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
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class StateTransferIT extends AbstractIT {

    private static String corfuSingleNodeHost;
    private final int basePort = 9000;
    private final int retries = 10;
    private CorfuRuntime firstRuntime;
    private CorfuRuntime secondRuntime;
    private CorfuRuntime writerRuntime;

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
    @SuppressWarnings("checkstyle:magicnumber")
    public void verifyStateTransferWithChainHeadFailure() throws Exception {
        verifyStateTransferWithNodeFailure(0);
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void verifyStateTransferWithChainTailFailure() throws Exception {
        verifyStateTransferWithNodeFailure(1);
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void verifyStateTransferWithChainHeadRestart() throws Exception {
        verifyStateTransferWithNodeRestart(0);
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void verifyStateTransferWithChainTailRestart() throws Exception {
        verifyStateTransferWithNodeRestart(1);
    }

    /**
     * A cluster of one nodes is started - 9000
     * Start a writer thread in background.
     * Then add node 9001 & 9002, remove them later.
     * Write a block of data to node 9000, and commit those data
     * Add other 2 nodes back. It triggers state transfer for two nodes.
     * Verify layout and all data is transferred.
     *
     * @throws Exception
     */
    @Test
    public void verifyRestorationWorkflow() throws Exception {
        final int PORT_0 = 9000;
        final int PORT_1 = 9001;
        final int PORT_2 = 9002;
        final Duration timeout = Duration.ofMinutes(5);
        final Duration pollPeriod = Duration.ofSeconds(5);
        final int workflowNumRetry = 5;
        final int nodesCount = 3;

        Process corfuServer_1 = runPersistentServer(corfuSingleNodeHost, PORT_0, false);
        Process corfuServer_2 = runPersistentServer(corfuSingleNodeHost, PORT_1, false);
        Process corfuServer_3 = runPersistentServer(corfuSingleNodeHost, PORT_2, false);
        List<Process> corfuServers = Arrays.asList(corfuServer_1, corfuServer_2, corfuServer_3);

        // bootstrap cluster with 1 node
        final Layout oneNodeLayout = getLayout(1);
        BootstrapUtil.bootstrap(oneNodeLayout, retries, PARAMETERS.TIMEOUT_SHORT);

        firstRuntime = createDefaultRuntime();
        waitForLayoutChange(layout -> layout.getAllServers().size() == 1
                        && layout.getSegments().size() == 1,
                firstRuntime);

        // start a writer future
        writerRuntime = createDefaultRuntime();
        final AtomicBoolean moreDataToBeWritten = new AtomicBoolean(true);
        CompletableFuture<Void> writerFuture = startWriter(moreDataToBeWritten);

        // add node 9001
        firstRuntime.getManagementView().addNode("localhost:9001", workflowNumRetry,
                timeout, pollPeriod);
        waitForLayoutChange(layout -> layout.getAllServers().size() == 2, firstRuntime);

        // add node 9002
        firstRuntime.getManagementView().addNode("localhost:9002", workflowNumRetry,
                timeout, pollPeriod);
        waitForLayoutChange(layout -> layout.getAllServers().size() == nodesCount, firstRuntime);

        // remove node 9002
        firstRuntime.getManagementView().removeNode("localhost:9002", workflowNumRetry,
                timeout, pollPeriod);
        waitForLayoutChange(layout -> layout.getAllServers().size() == 2, firstRuntime);

        // remove node 9001
        firstRuntime.getManagementView().removeNode("localhost:9001", workflowNumRetry,
                timeout, pollPeriod);
        waitForLayoutChange(layout -> layout.getAllServers().size() == 1, firstRuntime);

        // write a block of data
        final String data = createStringOfSize(100);
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            TokenResponse token = writerRuntime.getSequencerView().next();
            writerRuntime.getAddressSpaceView().write(token, data.getBytes());
        }

        // commit all written data
        long logTail = firstRuntime.getAddressSpaceView().getLogTail();
        firstRuntime.getAddressSpaceView().commit(0L, logTail);

        firstRuntime.getManagementView().addNode("localhost:9001", workflowNumRetry,
                timeout, pollPeriod);
        firstRuntime.getManagementView().addNode("localhost:9002", workflowNumRetry,
                timeout, pollPeriod);

        waitForLayoutChange(layout -> layout.getAllActiveServers().size() == nodesCount
                && layout.getUnresponsiveServers().isEmpty()
                && layout.getSegments().size() == 1, firstRuntime);

        moreDataToBeWritten.set(false);

        assertThatCode(writerFuture::join).doesNotThrowAnyException();

        // should have a greater committed tail after state transfer
        long newCommittedTail = firstRuntime.getAddressSpaceView().getCommittedTail();
        assertThat(newCommittedTail).isGreaterThan(logTail);

        verifyData(firstRuntime);

        for (Process server : corfuServers) {
            shutdownCorfuServer(server);
        }
    }

    /**
     * A cluster of one nodes is started - 9000
     * Start a writer thread in background.
     * Then add node 9001 & 9002, remove them later.
     * Write a block of data to node 9000, do prefixTrim and commit those data
     * Add other 2 nodes back. It triggers state transfer for two nodes.
     * Verify layout and all data is transferred.
     *
     * @throws Exception
     */
    @Test
    public void verifyRestorationWorkflowWithPrefixTrim() throws Exception {
        final int PORT_0 = 9000;
        final int PORT_1 = 9001;
        final int PORT_2 = 9002;
        final Duration timeout = Duration.ofMinutes(5);
        final Duration pollPeriod = Duration.ofSeconds(5);
        final int workflowNumRetry = 5;
        final int nodesCount = 3;

        Process corfuServer_1 = runPersistentServer(corfuSingleNodeHost, PORT_0, false);
        Process corfuServer_2 = runPersistentServer(corfuSingleNodeHost, PORT_1, false);
        Process corfuServer_3 = runPersistentServer(corfuSingleNodeHost, PORT_2, false);
        List<Process> corfuServers = Arrays.asList(corfuServer_1, corfuServer_2, corfuServer_3);

        // bootstrap cluster with 1 node
        final Layout oneNodeLayout = getLayout(1);
        BootstrapUtil.bootstrap(oneNodeLayout, retries, PARAMETERS.TIMEOUT_SHORT);

        firstRuntime = createDefaultRuntime();
        waitForLayoutChange(layout -> layout.getAllServers().size() == 1
                        && layout.getSegments().size() == 1,
                firstRuntime);

        // start a writer future
        writerRuntime = createDefaultRuntime();
        final AtomicBoolean moreDataToBeWritten = new AtomicBoolean(true);
        CompletableFuture<Void> writerFuture = startWriter(moreDataToBeWritten);

        // add node 9001
        firstRuntime.getManagementView().addNode("localhost:9001", workflowNumRetry,
                timeout, pollPeriod);
        waitForLayoutChange(layout -> layout.getAllServers().size() == 2, firstRuntime);

        // add node 9002
        firstRuntime.getManagementView().addNode("localhost:9002", workflowNumRetry,
                timeout, pollPeriod);
        waitForLayoutChange(layout -> layout.getAllServers().size() == nodesCount, firstRuntime);

        // remove node 9002
        firstRuntime.getManagementView().removeNode("localhost:9002", workflowNumRetry,
                timeout, pollPeriod);
        waitForLayoutChange(layout -> layout.getAllServers().size() == 2, firstRuntime);

        // remove node 9001
        firstRuntime.getManagementView().removeNode("localhost:9001", workflowNumRetry,
                timeout, pollPeriod);
        waitForLayoutChange(layout -> layout.getAllServers().size() == 1, firstRuntime);

        // write a block of data
        final String data = createStringOfSize(100);
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            TokenResponse token = writerRuntime.getSequencerView().next();
            writerRuntime.getAddressSpaceView().write(token, data.getBytes());
        }

        // do prefix trim
        long epoch = firstRuntime.getLayoutView().getLayout().getEpoch();
        long trimAddress = PARAMETERS.NUM_ITERATIONS_MODERATE / 2;
        firstRuntime.getAddressSpaceView().prefixTrim(Token.of(epoch, trimAddress));

        // commit all written data
        long logTail = firstRuntime.getAddressSpaceView().getLogTail();
        firstRuntime.getAddressSpaceView().commit(trimAddress + 1, logTail);

        firstRuntime.getManagementView().addNode("localhost:9001", workflowNumRetry,
                timeout, pollPeriod);
        firstRuntime.getManagementView().addNode("localhost:9002", workflowNumRetry,
                timeout, pollPeriod);

        waitForLayoutChange(layout -> layout.getAllActiveServers().size() == nodesCount
                && layout.getUnresponsiveServers().isEmpty()
                && layout.getSegments().size() == 1, firstRuntime);

        moreDataToBeWritten.set(false);

        assertThatCode(writerFuture::join).doesNotThrowAnyException();

        // should have a greater committed tail after state transfer
        long newCommittedTail = firstRuntime.getAddressSpaceView().getCommittedTail();
        assertThat(newCommittedTail).isGreaterThan(logTail);

        verifyData(firstRuntime);

        for (Process server : corfuServers) {
            shutdownCorfuServer(server);
        }
    }

    /**
     * A cluster of 3 nodes is started - 9000, 9001, 9002
     * Start a writer thread in background.
     * Then remove node 9001, 9002.
     * Write a block of data to node 9000, and commit those data
     * Add other 2 nodes back. It triggers state transfer for two nodes.
     * Restart node 9000 when node 9002 performing state transfer.
     * Verify layout and all data is transferred.
     *
     * @throws Exception
     */
    @Test
    public void verifyRestorationWorkflowWithFailure() throws Exception {
        final int PORT_0 = 9000;
        final int PORT_1 = 9001;
        final int PORT_2 = 9002;
        final Duration timeout = Duration.ofMinutes(5);
        final Duration pollPeriod = Duration.ofSeconds(5);
        final int workflowNumRetry = 5;
        final int nodesCount = 3;

        Process corfuServer_1 = runPersistentServer(corfuSingleNodeHost, PORT_0, false);
        Process corfuServer_2 = runPersistentServer(corfuSingleNodeHost, PORT_1, false);
        Process corfuServer_3 = runPersistentServer(corfuSingleNodeHost, PORT_2, false);
        List<Process> corfuServers = Arrays.asList(corfuServer_1, corfuServer_2, corfuServer_3);

        // bootstrap cluster with 3 node
        final Layout oneNodeLayout = getLayout(3);
        BootstrapUtil.bootstrap(oneNodeLayout, retries, PARAMETERS.TIMEOUT_SHORT);

        firstRuntime = createDefaultRuntime();
        waitForLayoutChange(layout -> layout.getAllServers().size() == nodesCount
                        && layout.getSegments().size() == 1,
                firstRuntime);

        // start a writer future
        writerRuntime = createDefaultRuntime();
        final AtomicBoolean moreDataToBeWritten = new AtomicBoolean(true);
        CompletableFuture<Void> writerFuture = startWriter(moreDataToBeWritten);

        // remove node 9002
        firstRuntime.getManagementView().removeNode("localhost:9002", workflowNumRetry,
                timeout, pollPeriod);
        waitForLayoutChange(layout -> layout.getAllServers().size() == 2, firstRuntime);

        // remove node 9001
        firstRuntime.getManagementView().removeNode("localhost:9001", workflowNumRetry,
                timeout, pollPeriod);
        waitForLayoutChange(layout -> layout.getAllServers().size() == 1, firstRuntime);

        // write a block of data
        final String data = createStringOfSize(100);
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            TokenResponse token = writerRuntime.getSequencerView().next();
            writerRuntime.getAddressSpaceView().write(token, data.getBytes());
        }

        // commit all written data
        long logTail = firstRuntime.getAddressSpaceView().getLogTail();
        firstRuntime.getAddressSpaceView().commit(0L, logTail);

        firstRuntime.getManagementView().addNode("localhost:9001", workflowNumRetry,
                timeout, pollPeriod);

        // kill node 9000 once node 9002 joins cluster
        secondRuntime = createDefaultRuntime();
        CompletableFuture<Void> killerFuture = CompletableFuture.runAsync(() -> {
            try {
                waitForLayoutChange(layout -> layout.getAllServers().size() == nodesCount, secondRuntime);
                assertThat(shutdownCorfuServer(corfuServer_1)).isTrue();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        firstRuntime.getManagementView().addNode("localhost:9002", workflowNumRetry,
                timeout, pollPeriod);
        assertThatCode(killerFuture::join).doesNotThrowAnyException();

        // wait for killed node becomes unresponsive and state transfer completes
        String killedNode = getServerEndpoint(PORT_0);
        waitForLayoutChange(layout -> layout.getAllServers().size() == nodesCount
                && layout.getUnresponsiveServers().contains(killedNode)
                && layout.getSegments().size() == 1, firstRuntime);

        // bring killed node back
        Process resumedServer = runPersistentServer(corfuSingleNodeHost, PORT_0, false);
        waitForLayoutChange(layout -> layout.getAllActiveServers().size() == nodesCount
                && layout.getUnresponsiveServers().isEmpty()
                && layout.getSegments().size() == 1, firstRuntime);

        moreDataToBeWritten.set(false);

        assertThatCode(writerFuture::join).doesNotThrowAnyException();

        // should have a greater committed tail after state transfer
        long newCommittedTail = firstRuntime.getAddressSpaceView().getCommittedTail();
        assertThat(newCommittedTail).isGreaterThan(logTail);

        verifyData(firstRuntime);

        shutdownCorfuServer(resumedServer);
        for (Process server : corfuServers) {
            shutdownCorfuServer(server);
        }
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
        final int workflowNumRetry = 5;
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

        waitForLayoutChange(layout -> layout.getAllServers().size() == 2
                        && layout.getSegments().size() == 1,
                firstRuntime);


        // write records to the 2 node cluster
        final String data = createStringOfSize(100);
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            TokenResponse token = writerRuntime.getSequencerView().next();
            writerRuntime.getAddressSpaceView().write(token, data.getBytes());
        }

        firstRuntime.getAddressSpaceView().commit(0, PARAMETERS.NUM_ITERATIONS_MODERATE - 1);

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
        firstRuntime.getManagementView().addNode("localhost:9002", workflowNumRetry,
                timeout, pollPeriod);

        assertThatCode(killerFuture::join).doesNotThrowAnyException();

        // wait for killed node becomes unresponsive and state transfer completes
        String killedNode = getServerEndpoint(basePort + killNode);
        waitForLayoutChange(layout -> layout.getAllServers().size() == nodesCount
                && layout.getUnresponsiveServers().contains(killedNode)
                && layout.getSegments().size() == 1, firstRuntime);

        // bring killed node back and verify data
        Process resumedServer = runPersistentServer(corfuSingleNodeHost, basePort + killNode, false);
        waitForLayoutChange(layout -> layout.getAllActiveServers().size() == nodesCount
                && layout.getUnresponsiveServers().isEmpty()
                && layout.getSegments().size() == 1, firstRuntime);

        moreDataToBeWritten.set(false);

        assertThatCode(writerFuture::join).doesNotThrowAnyException();

        // should have a greater committed tail after state transfer
        long prevCommittedTail = PARAMETERS.NUM_ITERATIONS_MODERATE - 1;
        long newCommittedTail = firstRuntime.getAddressSpaceView().getCommittedTail();
        assertThat(newCommittedTail).isGreaterThan(prevCommittedTail);

        verifyData(firstRuntime);

        shutdownCorfuServer(resumedServer);
        for (Process server : corfuServers) {
            shutdownCorfuServer(server);
        }
    }

    /**
     * A cluster of three nodes is started - 9000, 9001, 9002
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

        // should have a greater committed tail after state transfer
        long prevCommittedTail = PARAMETERS.NUM_ITERATIONS_MODERATE - 1;
        long newCommittedTail = getCommittedTail(firstRuntime);
        assertThat(newCommittedTail).isGreaterThan(prevCommittedTail);

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
                try {
                    writerRuntime.getAddressSpaceView().write(token, "Test Payload".getBytes());
                } catch (OverwriteException ignore) {
                    // ignore OverwriteException
                }
                Sleep.sleepUninterruptibly(Duration.ofMillis(timeOut));
            }
        });
    }

    /**
     * Queries for all the data in the 3 nodes and checks if the data state matches across the
     * cluster.
     *
     * @param corfuRuntime Connected instance of the runtime.
     * @throws Exception
     */
    private void verifyData(CorfuRuntime corfuRuntime) throws Exception {

        long lastAddress = corfuRuntime.getSequencerView().query().getSequence();
        long committedTail = corfuRuntime.getAddressSpaceView().getCommittedTail();
        long trimMark = corfuRuntime.getAddressSpaceView().getTrimMark().getSequence();
        Map<Long, LogData> map_0 = getAllData(corfuRuntime, "localhost:9000", trimMark, lastAddress);
        Map<Long, LogData> map_1 = getAllData(corfuRuntime, "localhost:9001", trimMark, lastAddress);
        Map<Long, LogData> map_2 = getAllData(corfuRuntime, "localhost:9002", trimMark, lastAddress);

        assertThat(getUncommittedDataCount(map_0, committedTail)).isZero();
        assertThat(getUncommittedDataCount(map_0, committedTail)).isZero();
        assertThat(getUncommittedDataCount(map_0, committedTail)).isZero();

        assertThat(filterEmptyEntry(map_1).entrySet()).containsExactlyElementsOf(filterEmptyEntry(map_0).entrySet());
        assertThat(filterEmptyEntry(map_2).entrySet()).containsExactlyElementsOf(filterEmptyEntry(map_0).entrySet());
    }

    /**
     * Fetches all the data from the node.
     *
     * @param corfuRuntime Connected instance of the runtime.
     * @param endpoint     Endpoint of query for all the data.
     * @param start        Start address from which data needs to be fetched.
     * @param end          End address up to which data needs to be fetched.
     * @return Map of all the addresses contained by the node corresponding to the data stored.
     * @throws Exception
     */
    private Map<Long, LogData> getAllData(CorfuRuntime corfuRuntime, String endpoint,
                                          long start, long end) throws Exception {
        ReadResponse readResponse = corfuRuntime.getLayoutView().getRuntimeLayout()
                .getLogUnitClient(endpoint)
                .readAll(getRangeAddressAsList(start, end))
                .get();
        return readResponse.getAddresses().entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private List<Long> getRangeAddressAsList(long startAddress, long endAddress) {
        Range<Long> range = Range.closed(startAddress, endAddress);
        return ContiguousSet.create(range, DiscreteDomain.longs()).asList();
    }

    private long getUncommittedDataCount(Map<Long, LogData> map, long committedTail) {
        return map.entrySet()
                .stream()
                .filter(longLogDataEntry -> longLogDataEntry.getKey() <= committedTail &&
                        longLogDataEntry.getValue().isEmpty())
                .count();
    }

    private Map<Long, LogData> filterEmptyEntry(Map<Long, LogData> map) {
        return map.entrySet()
                .stream()
                .filter(longLogDataEntry -> !longLogDataEntry.getValue().isEmpty())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private long getCommittedTail(CorfuRuntime runtime) {
        for (int i = 1; i <= retries; i++) {
            try {
                return runtime.getAddressSpaceView().getCommittedTail();
            } catch (Exception e) {
                runtime.invalidateLayout();
            }
        }

        return -1;
    }
}

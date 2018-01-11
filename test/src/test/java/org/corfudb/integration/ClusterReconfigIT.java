package org.corfudb.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.google.common.collect.Range;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.Layout;
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
        System.out.println("SEED = " + Long.toHexString(SEED));
        return new Random(SEED);
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

        Process corfuServer_1 = runSinglePersistentServer(corfuSingleNodeHost, PORT_0);
        Process corfuServer_2 = runUnbootstrappedPersistentServer(corfuSingleNodeHost, PORT_1);
        Process corfuServer_3 = runUnbootstrappedPersistentServer(corfuSingleNodeHost, PORT_2);

        CorfuRuntime runtime = createDefaultRuntime();

        Map<String, String> map = runtime.getObjectsView()
                .build()
                .setStreamName("test")
                .setType(SMRMap.class)
                .open();

        final String data = createStringOfSize(1_000);

        final int num = 15_000;

        Random r = getRandomNumberGenerator();
        for (int i = 0; i < num; i++) {
            String key = Long.toString(r.nextLong());
            map.put(key, data);
        }

        final AtomicBoolean moreDataToBeWritten = new AtomicBoolean(true);
        Thread t = new Thread(() -> {
            while (moreDataToBeWritten.get()) {
                assertThatCode(() -> {
                    try {
                        runtime.getObjectsView().TXBegin();
                        map.put(Integer.toString(r.nextInt()), data);
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

        runtime.getManagementView().addNode("localhost:9001");
        runtime.getManagementView().addNode("localhost:9002");

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
}

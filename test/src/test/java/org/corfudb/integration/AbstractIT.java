package org.corfudb.integration;

import static org.assertj.core.api.Assertions.assertThat;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.view.Layout;
import org.corfudb.test.CorfuServerRunner;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Integration tests.
 * Created by zlokhandwala on 4/28/17.
 */
@Slf4j
public class AbstractIT extends AbstractCorfuTest {

    static final String DEFAULT_HOST = CorfuServerRunner.DEFAULT_HOST;
    static final int DEFAULT_PORT = CorfuServerRunner.DEFAULT_PORT;
    static final String DEFAULT_ENDPOINT = DEFAULT_HOST + ":" + DEFAULT_PORT;

    static final String CORFU_PROJECT_DIR = new File("..").getAbsolutePath() + File.separator;
    static final String CORFU_LOG_PATH = PARAMETERS.TEST_TEMP_DIR;

    public CorfuRuntime runtime;

    public static final Properties PROPERTIES = new Properties();

    public static final String TEST_SEQUENCE_LOG_PATH = CORFU_LOG_PATH + File.separator + "testSequenceLog";

    public AbstractIT() {
        CorfuRuntime.overrideGetRouterFunction = null;
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream input = classLoader.getResourceAsStream("CorfuDB.properties");

        try {
            PROPERTIES.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Cleans up the corfu log directory before running any test.
     *
     * @throws Exception error
     */
    @Before
    public void setUp() throws Exception {
        runtime = null;
        CorfuServerRunner.forceShutdownAllCorfuServers();
        FileUtils.cleanDirectory(new File(CORFU_LOG_PATH));
    }

    /**
     * Cleans up all Corfu instances after the tests.
     *
     * @throws Exception error
     */
    @After
    public void cleanUp() throws Exception {
        CorfuServerRunner.forceShutdownAllCorfuServers();
        if (runtime != null) {
            runtime.shutdown();
        }
    }

    /**
     * Shuts down all corfu instances.
     *
     * @param corfuServerProcess
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public static boolean shutdownCorfuServer(Process corfuServerProcess) throws IOException, InterruptedException {
        return CorfuServerRunner.shutdownCorfuServer(corfuServerProcess);
    }

    /**
     * Refreshes the layout and waits for a limited time for the refreshed layout to
     * satisfy the expected verifier.
     *
     * @param verifier     Layout predicate to test the refreshed layout.
     * @param corfuRuntime corfu runtime.
     */
    public static void waitForLayoutChange(
            Predicate<Layout> verifier, CorfuRuntime corfuRuntime) throws InterruptedException {

        corfuRuntime.invalidateLayout();
        Layout refreshedLayout = corfuRuntime.getLayoutView().getLayout();

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            if (verifier.test(refreshedLayout)) {
                break;
            }
            corfuRuntime.invalidateLayout();
            refreshedLayout = corfuRuntime.getLayoutView().getLayout();
            TimeUnit.MILLISECONDS.sleep(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        }
        assertThat(verifier.test(refreshedLayout)).isTrue();
    }

    /**
     * Wait for the Supplier (some condition) to return true.
     *
     * @param supplier Supplier to test condition
     */
    public static void waitFor(Supplier<Boolean> supplier) throws InterruptedException {
        while (!supplier.get()) {
            TimeUnit.MILLISECONDS.sleep(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        }
    }

    public static CorfuRuntime createDefaultRuntime() {
        return createRuntime(DEFAULT_ENDPOINT);
    }

    public static Process runServer(int port, boolean single) throws IOException {
        return new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(port)
                .setSingle(single)
                .runServer();
    }

    public static Process runDefaultServer() throws IOException {
        return new CorfuServerRunner()
                .setSingle(true)
                .setLogPath(CorfuServerRunner.getCorfuServerLogPath(DEFAULT_HOST, DEFAULT_PORT))
                .runServer();
    }

    public static CorfuRuntime createRuntime(String endpoint) {
        CorfuRuntime rt = new CorfuRuntime(endpoint)
                .setCacheDisabled(true)
                .connect();
        return rt;
    }

    public static CorfuRuntime createRuntimeWithCache() {
        CorfuRuntime rt = new CorfuRuntime(DEFAULT_ENDPOINT)
                .setCacheDisabled(false)
                .connect();
        return rt;
    }

    public static Map<String, Integer> createMap(CorfuRuntime rt, String streamName) {
        return (Map<String, Integer>) rt.getObjectsView()
                .build()
                .setStreamName(streamName)
                .setType(SMRMap.class)
                .open();
    }
}

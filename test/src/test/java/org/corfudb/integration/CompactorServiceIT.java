package org.corfudb.integration;

import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.infrastructure.CompactorService;
import org.corfudb.infrastructure.DynamicTriggerPolicy;
import org.corfudb.infrastructure.InvokeCheckpointing;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.management.FailureDetector;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.PersistedCorfuTableTest;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.UnreachableClusterException;
import org.corfudb.runtime.exceptions.WrongClusterException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.view.AddressSpaceView;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CompactorServiceIT extends AbstractIT {
    private static String corfuSingleNodeHost;
    private static int corfuStringNodePort;
    private static String singleNodeEndpoint;
    private CompactorService compactorServiceSpy;
    private Process corfuServer;
    private CorfuRuntime runtime2;

    private final InvokeCheckpointing invokeCheckpointing = mock(InvokeCheckpointing.class);

    private static final Duration SCHEDULER_INTERVAL = Duration.ofSeconds(1);
    private static final Duration VERIFY_TIMEOUT = Duration.ofSeconds(20);

    @Before
    public void loadProperties() {
        corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        corfuStringNodePort = Integer.parseInt(PROPERTIES.getProperty("corfuSingleNodePort"));
        singleNodeEndpoint = String.format("%s:%d",
                corfuSingleNodeHost,
                corfuStringNodePort);
    }

    @After
    public void cleanUp() throws Exception {
        shutdownCorfuServer(corfuServer);
    }

    private Process runSinglePersistentServer(String host, int port, boolean disableLogUnitServerCache) throws IOException {
        return new AbstractIT.CorfuServerRunner()
                .setHost(host)
                .setPort(port)
                .setLogPath(getCorfuServerLogPath(host, port))
                .setSingle(true)
                .setDisableLogUnitServerCache(disableLogUnitServerCache)
                .runServer();
    }

    private CorfuRuntime createCompactorService() throws Exception {
        // Start Corfu Server
        corfuServer = runServer(DEFAULT_PORT, true);
        corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort, true);

        ServerContext sc = spy(new ServerContextBuilder()
                .setSingle(true)
                .setAddress(corfuSingleNodeHost)
                .setPort(DEFAULT_PORT)
                .setLogPath(com.google.common.io.Files.createTempDir().getAbsolutePath())
                .build());

        CorfuRuntime.CorfuRuntimeParameters.CorfuRuntimeParametersBuilder paramsBuilder = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .checkpointTriggerFreqMillis(1);
        doReturn(paramsBuilder.build()).when(sc).getManagementRuntimeParameters();
        compactorServiceSpy = spy(new CompactorService(sc, SCHEDULER_INTERVAL, invokeCheckpointing, new DynamicTriggerPolicy()));
        CorfuRuntime runtime = spy(createRuntime(singleNodeEndpoint, paramsBuilder));
        runtime.getParameters().setSystemDownHandler(compactorServiceSpy.getSystemDownHandlerForCompactor(runtime));
        doReturn(runtime).when(compactorServiceSpy).getNewCorfuRuntime();


        runtime2 = spy(createRuntime(singleNodeEndpoint, paramsBuilder));
        runtime2.getParameters().setSystemDownHandler(compactorServiceSpy.getSystemDownHandlerForCompactor(runtime2));

        return runtime;
    }

    @Test
    public void throwUnrecoverableCorfuErrorTest() throws Exception {
        CorfuRuntime runtime = createCompactorService();
        AddressSpaceView mockAddressSpaceView = spy(new AddressSpaceView(runtime));
        final Long address = 1L;
        doReturn(mockAddressSpaceView).when(runtime).getAddressSpaceView();
        doThrow(new UnrecoverableCorfuError(new InterruptedException("Thread interrupted"))).when(mockAddressSpaceView).read(eq(address), any(), any());
        compactorServiceSpy.start(SCHEDULER_INTERVAL);
        await().untilAsserted(() -> verify(compactorServiceSpy, atLeast(2)).start(any()));
        verify(compactorServiceSpy).shutdown();
    }

    @Test
    public void invokeSystemDownHandlerOnExceptionTest() throws Exception {
        CorfuRuntime runtime = createCompactorService();
        doCallRealMethod().doCallRealMethod().doCallRealMethod()
                .doThrow(new WrongClusterException(UUID.randomUUID(), UUID.randomUUID()))
                .doCallRealMethod().when(runtime).checkClusterId(any());
        compactorServiceSpy.start(SCHEDULER_INTERVAL);

        await().untilAsserted(() -> verify(compactorServiceSpy, times(2)).start(any()));
        verify(compactorServiceSpy).shutdown();
    }

    @Test
    public void invokeConcurrentSystemDownHandlerTest() throws Exception {
        CorfuRuntime runtime = createCompactorService();

        CorfuStore corfuStore = mock(CorfuStore.class);
        TxnContext txn = mock(TxnContext.class);
        CorfuStoreEntry corfuStoreEntry = mock(CorfuStoreEntry.class);
        doReturn(txn).when(corfuStore).txn(any());
        doReturn(corfuStoreEntry).when(txn).getRecord(anyString(), any());
        doReturn(null).when(corfuStoreEntry).getPayload();
        doReturn(corfuStore).when(compactorServiceSpy).getCorfuStore();

        //return runtime2 when systemHandler is invoked the 2nd time
        doReturn(runtime).doReturn(runtime2).when(compactorServiceSpy).getNewCorfuRuntime();
        compactorServiceSpy.start(SCHEDULER_INTERVAL);

        verify(compactorServiceSpy, timeout(VERIFY_TIMEOUT.toMillis()).times(1)).getNewCorfuRuntime();

        Runnable invokeConcurrentSystemDownHandler = () -> runtime.getParameters().getSystemDownHandler().run();

        Thread t1 = new Thread(invokeConcurrentSystemDownHandler);
        Thread t2 = new Thread(invokeConcurrentSystemDownHandler);
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        verify(compactorServiceSpy, timeout(VERIFY_TIMEOUT.toMillis())).getCompactorLeaderServices();
        verify(compactorServiceSpy, times(2)).getSystemDownHandlerForCompactor(any());
        verify(runtime, times(2)).shutdown();
        verify(compactorServiceSpy, times(1)).shutdown();

        // Note that start() was called once manually above.
        verify(compactorServiceSpy, times(2)).start(any());
    }

    /**
     * Test that the CompactorService runtime is shutdown when there is a
     * failure (UnrecoverableCorfuError) on connect() to CorfuDB.
     */
    @Test
    public void testConnectCleanup() throws Exception {
        corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort, true);
        final ServerContext scSpy = spy(new ServerContextBuilder()
                .setSingle(true)
                .setAddress(corfuSingleNodeHost)
                .setPort(corfuStringNodePort)
                .setLogPath(com.google.common.io.Files.createTempDir().getAbsolutePath())
                .build());

        // Populate the CorfuRuntimeParameters
        CorfuRuntime.CorfuRuntimeParameters.CorfuRuntimeParametersBuilder paramsBuilder =
                CorfuRuntime.CorfuRuntimeParameters.builder().checkpointTriggerFreqMillis(1);

        doReturn(paramsBuilder.build()).when(scSpy).getManagementRuntimeParameters();

        CompactorService compactorService = new CompactorService(scSpy, SCHEDULER_INTERVAL, invokeCheckpointing, new DynamicTriggerPolicy());

        // Inject our CorfuRuntime into the test.
        CorfuRuntime rt = spy(CorfuRuntime.fromParameters(paramsBuilder.build())
                .parseConfigurationString(singleNodeEndpoint));

        try (MockedStatic<CorfuRuntime> mockedStatic = mockStatic(CorfuRuntime.class)) {
            doThrow(new UnrecoverableCorfuError("Fatal error connecting to Corfu server instance.")).when(rt).connect();
            doNothing().when(rt).shutdown();

            mockedStatic.when(() -> CorfuRuntime.fromParameters(any())).thenReturn(rt);

            // Validate that the systemDownHandler was invoked.
             try {
                 compactorService.getNewCorfuRuntime();
             } catch (Throwable th) {
                 assertThat(th).isInstanceOf(UnreachableClusterException.class);
             }

             // Validate that shutdown was called exactly once.
             verify(rt, times(1)).shutdown();
        }
    }

    // The detection result can be seen in the log
    @Test
    public void detectLatencyAnomalyTest() throws Exception {
        createCompactorService();
        compactorServiceSpy.start(Duration.ofSeconds(19));
        verify(compactorServiceSpy, timeout(VERIFY_TIMEOUT.toMillis()).times(1)).getNewCorfuRuntime();
    }
}

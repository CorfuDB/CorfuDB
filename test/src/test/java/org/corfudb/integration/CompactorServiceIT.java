package org.corfudb.integration;

import org.corfudb.infrastructure.CompactorService;
import org.corfudb.infrastructure.DynamicTriggerPolicy;
import org.corfudb.infrastructure.InvokeCheckpointing;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.WrongClusterException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.view.AddressSpaceView;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
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
        corfuStringNodePort = Integer.valueOf(PROPERTIES.getProperty("corfuSingleNodePort"));
        singleNodeEndpoint = String.format("%s:%d",
                corfuSingleNodeHost,
                corfuStringNodePort);
    }

    @After
    public void cleanUp() throws IOException, InterruptedException {
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

    private void createCompactorService() throws Exception {
        // Start Corfu Server
        corfuServer = runServer(DEFAULT_PORT, true);
        corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort, true);

        ServerContext sc = spy(new ServerContextBuilder()
                .setSingle(true)
                .setAddress(corfuSingleNodeHost)
                .setPort(DEFAULT_PORT)
                .setLogPath(com.google.common.io.Files.createTempDir().getAbsolutePath())
                .build());
        compactorServiceSpy = spy(new CompactorService(sc, SCHEDULER_INTERVAL, invokeCheckpointing, new DynamicTriggerPolicy()));
        CorfuRuntime.CorfuRuntimeParameters.CorfuRuntimeParametersBuilder paramsBuilder = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .checkpointTriggerFreqMillis(1);
        runtime = spy(createRuntime(singleNodeEndpoint, paramsBuilder));
        runtime.getParameters().setSystemDownHandler(compactorServiceSpy.getSystemDownHandlerForCompactor(runtime));
        doReturn(runtime).when(compactorServiceSpy).getNewCorfuRuntime();

        runtime2 = spy(createRuntime(singleNodeEndpoint, paramsBuilder));
        runtime2.getParameters().setSystemDownHandler(compactorServiceSpy.getSystemDownHandlerForCompactor(runtime2));
    }

    @Test
    public void throwUnrecoverableCorfuErrorTest() throws Exception {
        createCompactorService();
        AddressSpaceView mockAddressSpaceView = spy(new AddressSpaceView(runtime));
        Long address = 7L;
        doReturn(mockAddressSpaceView).when(runtime).getAddressSpaceView();
        doThrow(new UnrecoverableCorfuError(new InterruptedException("Thread interrupted"))).when(mockAddressSpaceView).read(eq(address), any(), any());
        compactorServiceSpy.start(SCHEDULER_INTERVAL);

        verify(compactorServiceSpy, timeout(VERIFY_TIMEOUT.toMillis()).atLeast(2)).start(any());
        verify(compactorServiceSpy).shutdown();
    }

    @Test
    public void invokeSystemDownHandlerOnExceptionTest() throws Exception {
        createCompactorService();
        doCallRealMethod().doCallRealMethod().doCallRealMethod()
                .doThrow(new WrongClusterException(UUID.randomUUID(), UUID.randomUUID()))
                .doCallRealMethod().when(runtime).checkClusterId(any());
        compactorServiceSpy.start(SCHEDULER_INTERVAL);

        verify(compactorServiceSpy, timeout(VERIFY_TIMEOUT.toMillis()).times(2)).start(any());
        verify(compactorServiceSpy).shutdown();
    }

    @Test
    public void invokeConcurrentSystemDownHandlerTest() throws Exception {
        createCompactorService();

        //return runtime2 when systemHandler is invoked the 2nd time
        doReturn(runtime).doReturn(runtime2).when(compactorServiceSpy).getNewCorfuRuntime();
        compactorServiceSpy.start(SCHEDULER_INTERVAL);

        verify(compactorServiceSpy, timeout(VERIFY_TIMEOUT.toMillis()).atLeastOnce()).getNewCorfuRuntime();

        Runnable invokeConcurrentSystemDownHandler = () -> {
            runtime.getParameters().getSystemDownHandler().run();
        };

        Thread t1 = new Thread(invokeConcurrentSystemDownHandler);
        Thread t2 = new Thread(invokeConcurrentSystemDownHandler);
        t1.start(); t2.start();
        t1.join(); t2.join();

        verify(compactorServiceSpy, timeout(VERIFY_TIMEOUT.toMillis()).times(2)).start(any());
        verify(compactorServiceSpy).shutdown();
        verify(compactorServiceSpy, times(2)).getSystemDownHandlerForCompactor(any());
    }
}

package org.corfudb.infrastructure.health;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.integration.AbstractIT;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.QuotaExceededException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.health.HealthReport.ReportedHealthStatus;
import static org.corfudb.infrastructure.health.HealthReport.builder;

@Slf4j
public class HealthMonitorIT extends AbstractIT {

    private static final int CORFU_PORT = 9000;
    private static final int HEALTH_PORT = 8080;
    private static final String ADDRESS = "localhost";
    private static final int RETRIES = 3;
    private static final int WAIT_TIME_MILLIS = 1000;

    private Process runCorfuServerWithHealthMonitor() throws IOException {
        return new CorfuServerRunner()
                .setHost(ADDRESS)
                .setPort(CORFU_PORT)
                .setHealthPort(HEALTH_PORT)
                .setLogPath(getCorfuServerLogPath(ADDRESS, CORFU_PORT))
                .setSingle(false)
                .runServer();
    }

    private Process runCorfuServerWithHealthMonitorAndExceededQuota() throws IOException {
        return new CorfuServerRunner()
                .setHost(ADDRESS)
                .setPort(CORFU_PORT)
                .setHealthPort(HEALTH_PORT)
                .setLogSizeLimitPercentage("0.00000001")
                .setLogPath(getCorfuServerLogPath(ADDRESS, CORFU_PORT))
                .setSingle(false)
                .runServer();
    }

    private Layout getLayout() {
        List<String> servers = new ArrayList<>();
        String serverAddress = ADDRESS + ":" + CORFU_PORT;
        servers.add(serverAddress);

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

    @SuppressWarnings("checkstyle:magicnumber")
    private HealthReport queryCurrentHealthReport() throws IOException {
        URL url = new URL("http://" + ADDRESS + ":" + HEALTH_PORT + "/health");
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setRequestMethod("GET");
        urlConnection.setRequestProperty("Content-Type", "application/json");
        urlConnection.setConnectTimeout(1000);
        urlConnection.setReadTimeout(1000);
        urlConnection.connect();
        int status = urlConnection.getResponseCode();
        String json;
        if (status == 200 || status == 201) {
            BufferedReader br = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line + "\n");
            }
            br.close();
            json = sb.toString();

        } else {
            throw new IllegalStateException("Unable to connect");
        }
        Gson gson = new Gson();

        final HealthReport healthReport = gson.fromJson(json, HealthReport.class);
        return healthReport;
    }

    private LogData getLogData(TokenResponse token, byte[] payload) {
        LogData ld = new LogData(DataType.DATA, payload);
        ld.useToken(token);
        return ld;
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    void testInitComponentsHealth() throws IOException, InterruptedException {
        Process corfuServer = runCorfuServerWithHealthMonitor();
        HealthReport expectedHealthReport = builder()
                .status(false)
                .reason("Some of the services are not initialized")
                .init(ImmutableMap.of(
                        Component.LOG_UNIT, new ReportedHealthStatus(false, "Service is not initialized"),
                        Component.LAYOUT_SERVER, new ReportedHealthStatus(false, "Service is not initialized"),
                        Component.ORCHESTRATOR, new ReportedHealthStatus(true, "Initialization successful"),
                        Component.FAILURE_DETECTOR, new ReportedHealthStatus(false, "Service is not initialized"),
                        Component.SEQUENCER, new ReportedHealthStatus(false, "Service is not initialized")
                ))
                .runtime(ImmutableMap.of(
                        Component.LOG_UNIT, new ReportedHealthStatus(false, "Service is not running"),
                        Component.LAYOUT_SERVER, new ReportedHealthStatus(false, "Service is not running"),
                        Component.ORCHESTRATOR, new ReportedHealthStatus(true, "Up and running"),
                        Component.FAILURE_DETECTOR, new ReportedHealthStatus(false, "Service is not running"),
                        Component.SEQUENCER, new ReportedHealthStatus(false, "Service is not running")
                ))
                .build();
        Thread.sleep(WAIT_TIME_MILLIS * 3);
        assertThat(queryCurrentHealthReport()).isEqualTo(expectedHealthReport);

        // Bootstrap corfu - services become healthy
        BootstrapUtil.bootstrap(getLayout(), RETRIES, PARAMETERS.TIMEOUT_SHORT);
        Thread.sleep(WAIT_TIME_MILLIS * 2);
        expectedHealthReport = builder()
                .status(true)
                .reason("Healthy")
                .init(ImmutableMap.of(
                        Component.LOG_UNIT, new ReportedHealthStatus(true, "Initialization successful"),
                        Component.LAYOUT_SERVER, new ReportedHealthStatus(true, "Initialization successful"),
                        Component.ORCHESTRATOR, new ReportedHealthStatus(true, "Initialization successful"),
                        Component.FAILURE_DETECTOR, new ReportedHealthStatus(true, "Initialization successful"),
                        Component.SEQUENCER, new ReportedHealthStatus(true, "Initialization successful")
                ))
                .runtime(ImmutableMap.of(
                        Component.LOG_UNIT, new ReportedHealthStatus(true, "Up and running"),
                        Component.LAYOUT_SERVER, new ReportedHealthStatus(true, "Up and running"),
                        Component.ORCHESTRATOR, new ReportedHealthStatus(true, "Up and running"),
                        Component.FAILURE_DETECTOR, new ReportedHealthStatus(true, "Up and running"),
                        Component.SEQUENCER, new ReportedHealthStatus(true, "Up and running")
                ))
                .build();
        assertThat(queryCurrentHealthReport()).isEqualTo(expectedHealthReport);

        // Kill the process and start again - corfu still should be healthy because it's bootstrapped
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
        Process restartedServer = runCorfuServerWithHealthMonitor();
        Thread.sleep(WAIT_TIME_MILLIS * 3);
        assertThat(queryCurrentHealthReport()).isEqualTo(expectedHealthReport);
        assertThat(shutdownCorfuServer(restartedServer)).isTrue();
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    void testQuotaExceededReport() throws IOException, InterruptedException {
        final Process process = runCorfuServerWithHealthMonitorAndExceededQuota();
        BootstrapUtil.bootstrap(getLayout(), RETRIES, PARAMETERS.TIMEOUT_SHORT);
        final CorfuRuntime defaultRuntime = createDefaultRuntime();
        while (true) {
            TokenResponse token = defaultRuntime.getSequencerView().next();
            try {
                CFUtils.getUninterruptibly(defaultRuntime.getLayoutView().getRuntimeLayout()
                        .getLogUnitClient("localhost:9000")
                        .write(getLogData(token, "some data".getBytes())), QuotaExceededException.class);
            }
            catch (QuotaExceededException qee) {
                break;
            }

        }
        Thread.sleep(WAIT_TIME_MILLIS * 2);
        HealthReport expectedHealthReport = builder()
                .status(false)
                .reason("Some of the services experience runtime health issues")
                .init(ImmutableMap.of(
                        Component.LOG_UNIT, new ReportedHealthStatus(true, "Initialization successful"),
                        Component.LAYOUT_SERVER, new ReportedHealthStatus(true, "Initialization successful"),
                        Component.ORCHESTRATOR, new ReportedHealthStatus(true, "Initialization successful"),
                        Component.FAILURE_DETECTOR, new ReportedHealthStatus(true, "Initialization successful"),
                        Component.SEQUENCER, new ReportedHealthStatus(true, "Initialization successful")
                ))
                .runtime(ImmutableMap.of(
                        Component.LOG_UNIT, new ReportedHealthStatus(false, "Quota exceeded"),
                        Component.LAYOUT_SERVER, new ReportedHealthStatus(true, "Up and running"),
                        Component.ORCHESTRATOR, new ReportedHealthStatus(true, "Up and running"),
                        Component.FAILURE_DETECTOR, new ReportedHealthStatus(true, "Up and running"),
                        Component.SEQUENCER, new ReportedHealthStatus(true, "Up and running")
                ))
                .build();

        HealthReport healthReport = queryCurrentHealthReport();
        assertThat(healthReport).isEqualTo(expectedHealthReport);
        assertThat(shutdownCorfuServer(process)).isTrue();
        // Bring corfu again and verify that the issue persists
        Process anotherProcess = runCorfuServerWithHealthMonitorAndExceededQuota();
        Thread.sleep(WAIT_TIME_MILLIS * 2);
        healthReport = queryCurrentHealthReport();
        assertThat(healthReport).isEqualTo(expectedHealthReport);
        assertThat(shutdownCorfuServer(anotherProcess)).isTrue();
        // Now bring corfu such that the quota is not exceed
        Process anotherProcessAgain = runCorfuServerWithHealthMonitor();
        Thread.sleep(WAIT_TIME_MILLIS * 2);
        healthReport = queryCurrentHealthReport();
        expectedHealthReport = builder()
                .status(true)
                .reason("Healthy")
                .init(ImmutableMap.of(
                        Component.LOG_UNIT, new ReportedHealthStatus(true, "Initialization successful"),
                        Component.LAYOUT_SERVER, new ReportedHealthStatus(true, "Initialization successful"),
                        Component.ORCHESTRATOR, new ReportedHealthStatus(true, "Initialization successful"),
                        Component.FAILURE_DETECTOR, new ReportedHealthStatus(true, "Initialization successful"),
                        Component.SEQUENCER, new ReportedHealthStatus(true, "Initialization successful")
                ))
                .runtime(ImmutableMap.of(
                        Component.LOG_UNIT, new ReportedHealthStatus(true, "Up and running"),
                        Component.LAYOUT_SERVER, new ReportedHealthStatus(true, "Up and running"),
                        Component.ORCHESTRATOR, new ReportedHealthStatus(true, "Up and running"),
                        Component.FAILURE_DETECTOR, new ReportedHealthStatus(true, "Up and running"),
                        Component.SEQUENCER, new ReportedHealthStatus(true, "Up and running")
                ))
                .build();
        assertThat(healthReport).isEqualTo(expectedHealthReport);
        assertThat(shutdownCorfuServer(anotherProcessAgain)).isTrue();
    }

}

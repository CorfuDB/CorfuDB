package org.corfudb.infrastructure.health;

import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.health.HealthReport.ComponentReportedHealthStatus;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.health.Component.FAILURE_DETECTOR;
import static org.corfudb.infrastructure.health.Component.LAYOUT_SERVER;
import static org.corfudb.infrastructure.health.Component.LOG_UNIT;
import static org.corfudb.infrastructure.health.Component.ORCHESTRATOR;
import static org.corfudb.infrastructure.health.Component.SEQUENCER;
import static org.corfudb.infrastructure.health.HealthReport.COMPONENT_INITIALIZED;
import static org.corfudb.infrastructure.health.HealthReport.COMPONENT_IS_NOT_RUNNING;
import static org.corfudb.infrastructure.health.HealthReport.COMPONENT_IS_RUNNING;
import static org.corfudb.infrastructure.health.HealthReport.COMPONENT_NOT_INITIALIZED;
import static org.corfudb.infrastructure.health.HealthReport.ComponentStatus.DOWN;
import static org.corfudb.infrastructure.health.HealthReport.ComponentStatus.FAILURE;
import static org.corfudb.infrastructure.health.HealthReport.ComponentStatus.UP;
import static org.corfudb.infrastructure.health.HealthReport.OVERALL_STATUS_DOWN;
import static org.corfudb.infrastructure.health.HealthReport.OVERALL_STATUS_FAILURE;
import static org.corfudb.infrastructure.health.HealthReport.OVERALL_STATUS_UP;
import static org.corfudb.infrastructure.health.HealthReport.builder;

@Slf4j
public class HealthMonitorIT extends AbstractIT {

    private static final int CORFU_PORT_1 = 9000;
    private static final int CORFU_PORT_2 = 9001;
    private static final int HEALTH_PORT_1 = 8080;
    private static final int HEALTH_PORT_2 = 8081;
    private static final String ADDRESS = "localhost";
    private static final int RETRIES = 3;
    private static final int WAIT_TIME_MILLIS = 1000;

    private Process runCorfuServerWithHealthMonitor(int port, int healthPort) throws IOException {
        return new CorfuServerRunner()
                .setHost(ADDRESS)
                .setPort(port)
                .setHealthPort(healthPort)
                .setLogPath(getCorfuServerLogPath(ADDRESS, port))
                .setSingle(false)
                .runServer();
    }

    private Process runCorfuServerWithHealthMonitorAndExceededQuota(int port, int healthPort) throws IOException {
        return new CorfuServerRunner()
                .setHost(ADDRESS)
                .setPort(port)
                .setHealthPort(healthPort)
                .setLogSizeLimitPercentage("0.00000001")
                .setLogPath(getCorfuServerLogPath(ADDRESS, port))
                .setSingle(false)
                .runServer();
    }

    private Layout getLayout(int port) {
        List<String> servers = new ArrayList<>();
        String serverAddress = ADDRESS + ":" + port;
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
    private HealthReport queryCurrentHealthReport(int healthPort) throws IOException {
        URL url = new URL("http://" + ADDRESS + ":" + healthPort + "/health");
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
        Process corfuServer = runCorfuServerWithHealthMonitor(CORFU_PORT_1, HEALTH_PORT_1);
        HealthReport expectedHealthReport = builder()
                .status(DOWN)
                .reason(OVERALL_STATUS_DOWN)
                .init(ImmutableSet.of(
                        new ComponentReportedHealthStatus(LOG_UNIT, DOWN, COMPONENT_NOT_INITIALIZED),
                        new ComponentReportedHealthStatus(LAYOUT_SERVER, DOWN, COMPONENT_NOT_INITIALIZED),
                        new ComponentReportedHealthStatus(ORCHESTRATOR, DOWN, COMPONENT_NOT_INITIALIZED),
                        new ComponentReportedHealthStatus(FAILURE_DETECTOR, DOWN, COMPONENT_NOT_INITIALIZED),
                        new ComponentReportedHealthStatus(SEQUENCER, DOWN, COMPONENT_NOT_INITIALIZED)))
                .runtime(ImmutableSet.of(
                        new ComponentReportedHealthStatus(LOG_UNIT, DOWN, COMPONENT_IS_NOT_RUNNING),
                        new ComponentReportedHealthStatus(LAYOUT_SERVER, DOWN, COMPONENT_IS_NOT_RUNNING),
                        new ComponentReportedHealthStatus(ORCHESTRATOR, DOWN, COMPONENT_IS_NOT_RUNNING),
                        new ComponentReportedHealthStatus(FAILURE_DETECTOR, DOWN, COMPONENT_IS_NOT_RUNNING),
                        new ComponentReportedHealthStatus(SEQUENCER, DOWN, COMPONENT_IS_NOT_RUNNING)))
                .build();
        Thread.sleep(WAIT_TIME_MILLIS * 3);

        assertThat(queryCurrentHealthReport(HEALTH_PORT_1)).isEqualTo(expectedHealthReport);

        // Bootstrap corfu - services become healthy
        BootstrapUtil.bootstrap(getLayout(CORFU_PORT_1), RETRIES, PARAMETERS.TIMEOUT_SHORT);
        Thread.sleep(WAIT_TIME_MILLIS * 2);
        expectedHealthReport = builder()
                .status(UP)
                .reason(OVERALL_STATUS_UP)
                .init(ImmutableSet.of(
                        new ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_INITIALIZED),
                        new ComponentReportedHealthStatus(LAYOUT_SERVER, UP, COMPONENT_INITIALIZED),
                        new ComponentReportedHealthStatus(ORCHESTRATOR, UP, COMPONENT_INITIALIZED),
                        new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_INITIALIZED),
                        new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_INITIALIZED)))
                .runtime(ImmutableSet.of(
                        new ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_IS_RUNNING),
                        new ComponentReportedHealthStatus(LAYOUT_SERVER, UP, COMPONENT_IS_RUNNING),
                        new ComponentReportedHealthStatus(ORCHESTRATOR, UP, COMPONENT_IS_RUNNING),
                        new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_IS_RUNNING),
                        new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_IS_RUNNING)))
                .build();

        assertThat(queryCurrentHealthReport(HEALTH_PORT_1)).isEqualTo(expectedHealthReport);

        // Kill the process and start again - corfu still should be healthy because it's bootstrapped
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
        Process restartedServer = runCorfuServerWithHealthMonitor(CORFU_PORT_1, HEALTH_PORT_1);
        Thread.sleep(WAIT_TIME_MILLIS * 3);
        assertThat(queryCurrentHealthReport(HEALTH_PORT_1)).isEqualTo(expectedHealthReport);
        assertThat(shutdownCorfuServer(restartedServer)).isTrue();
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    void testQuotaExceededReport() throws IOException, InterruptedException {
        final Process process = runCorfuServerWithHealthMonitorAndExceededQuota(CORFU_PORT_2, HEALTH_PORT_2);
        BootstrapUtil.bootstrap(getLayout(CORFU_PORT_2), RETRIES, PARAMETERS.TIMEOUT_SHORT);
        final CorfuRuntime defaultRuntime = createRuntime("localhost:" +  CORFU_PORT_2);
        while (true) {
            TokenResponse token = defaultRuntime.getSequencerView().next();
            try {
                CFUtils.getUninterruptibly(defaultRuntime.getLayoutView().getRuntimeLayout()
                        .getLogUnitClient("localhost:" +  CORFU_PORT_2)
                        .write(getLogData(token, "some data".getBytes())), QuotaExceededException.class);
            }
            catch (QuotaExceededException qee) {
                break;
            }

        }
        Thread.sleep(WAIT_TIME_MILLIS * 2);
        HealthReport expectedHealthReport = builder()
                .status(FAILURE)
                .reason(OVERALL_STATUS_FAILURE)
                .init(ImmutableSet.of(
                        new ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_INITIALIZED),
                        new ComponentReportedHealthStatus(LAYOUT_SERVER, UP, COMPONENT_INITIALIZED),
                        new ComponentReportedHealthStatus(ORCHESTRATOR, UP, COMPONENT_INITIALIZED),
                        new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_INITIALIZED),
                        new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_INITIALIZED)))
                .runtime(ImmutableSet.of(
                        new ComponentReportedHealthStatus(LOG_UNIT, FAILURE, "Quota exceeded"),
                        new ComponentReportedHealthStatus(LAYOUT_SERVER, UP, COMPONENT_INITIALIZED),
                        new ComponentReportedHealthStatus(ORCHESTRATOR, UP, COMPONENT_INITIALIZED),
                        new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_INITIALIZED),
                        new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_INITIALIZED)))
                .build();

        HealthReport healthReport = queryCurrentHealthReport(HEALTH_PORT_2);
        assertThat(healthReport).isEqualTo(expectedHealthReport);
        assertThat(shutdownCorfuServer(process)).isTrue();
        // Bring corfu again and verify that the issue persists
        Process anotherProcess = runCorfuServerWithHealthMonitorAndExceededQuota(CORFU_PORT_2, HEALTH_PORT_2);
        Thread.sleep(WAIT_TIME_MILLIS * 2);
        healthReport = queryCurrentHealthReport(HEALTH_PORT_2);
        assertThat(healthReport).isEqualTo(expectedHealthReport);
        assertThat(shutdownCorfuServer(anotherProcess)).isTrue();
        // Now bring corfu such that the quota is not exceed
        Process anotherProcessAgain = runCorfuServerWithHealthMonitor(CORFU_PORT_2, HEALTH_PORT_2);
        Thread.sleep(WAIT_TIME_MILLIS * 2);
        healthReport = queryCurrentHealthReport(HEALTH_PORT_2);
        expectedHealthReport = builder()
                .status(UP)
                .reason(OVERALL_STATUS_UP)
                .init(ImmutableSet.of(
                        new ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_INITIALIZED),
                        new ComponentReportedHealthStatus(LAYOUT_SERVER, UP, COMPONENT_INITIALIZED),
                        new ComponentReportedHealthStatus(ORCHESTRATOR, UP, COMPONENT_INITIALIZED),
                        new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_INITIALIZED),
                        new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_INITIALIZED)))
                .runtime(ImmutableSet.of(
                        new ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_IS_RUNNING),
                        new ComponentReportedHealthStatus(LAYOUT_SERVER, UP, COMPONENT_IS_RUNNING),
                        new ComponentReportedHealthStatus(ORCHESTRATOR, UP, COMPONENT_IS_RUNNING),
                        new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_IS_RUNNING),
                        new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_IS_RUNNING)))
                .build();
        assertThat(healthReport).isEqualTo(expectedHealthReport);
        assertThat(shutdownCorfuServer(anotherProcessAgain)).isTrue();
        defaultRuntime.shutdown();
    }

}

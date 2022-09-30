package org.corfudb.universe;

import com.google.gson.Gson;
import org.corfudb.infrastructure.health.HealthReport;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.universe.UniverseManager.UniverseWorkflow;
import org.corfudb.universe.scenario.fixture.Fixture;
import org.corfudb.universe.universe.Universe.UniverseMode;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.util.Sleep;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.function.Consumer;

/**
 * Common parent class for all universe tests.
 * Each test should extend GenericIntegrationTest class.
 * The class provides the initialization steps, it creates UniverseManager that is used to provide
 * a universe framework workflow
 * to manage your initialization and deployment process in the universe.
 */
public abstract class GenericIntegrationTest {
    private static final UniverseAppUtil APP_UTIL = new UniverseAppUtil();

    @Rule
    public TestName test = new TestName();

    private UniverseManager universeManager;

    private static final int RETRIES = 3;

    private static final int WAIT_TIME_MILLIS = 3000;

    @Before
    public void setUp() {
        universeManager = UniverseManager.builder()
                .testName(test.getMethodName())
                .universeMode(UniverseMode.DOCKER)
                .corfuServerVersion(APP_UTIL.getAppVersion())
                .build();
    }

    public <T extends Fixture<UniverseParams>> UniverseWorkflow workflow(
            Consumer<UniverseWorkflow<T>> action) {

        return universeManager.workflow(action);
    }


    public HealthReport queryHealthReport(int healthPort) {
        for (int i = 0; i < RETRIES; i++) {
            try {
                return queryHealthReportHelper(healthPort);
            }
            catch (Exception e) {
                Sleep.sleepUninterruptibly(Duration.ofMillis(WAIT_TIME_MILLIS));
            }
        }
        throw new RetryExhaustedException("Could not get the health report within the provided time");
    }

    private HealthReport queryHealthReportHelper(int port) {
        try {
            URL url = new URL("http://localhost:" + port + "/health");
            HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.setRequestMethod("GET");
            urlConnection.setRequestProperty("Content-Type", "application/json");
            urlConnection.setConnectTimeout(3000);
            urlConnection.setReadTimeout(3000);
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
        catch (IOException  io) {
            throw new IllegalStateException(io);
        }
    }

}

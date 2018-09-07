package org.corfudb.universe.scenario;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import org.corfudb.universe.Universe;
import org.corfudb.universe.cluster.Cluster.ClusterParams;
import org.corfudb.universe.cluster.docker.DockerCluster;
import org.corfudb.universe.scenario.config.ScenarioConfig;
import org.corfudb.universe.scenario.fixture.Fixtures.ClusterFixture;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class ScenarioIT {
    private static final Universe UNIVERSE = Universe.getInstance();
    private static final ObjectMapper JSON = new ObjectMapper();


    private DockerCluster dockerCluster;
    private final DockerClient docker;

    public ScenarioIT() throws Exception {
        this.docker = DefaultDockerClient.fromEnv().build();
    }

    @BeforeClass
    public static void init() {
        JSON.registerModule(new Jdk8Module());
        JSON.registerModule(new GuavaModule());
    }

    @After
    public void tearDown() {
        if (dockerCluster != null) {
            dockerCluster.shutdown();
        }
    }

    @Test
    public void scenarioTest() throws Exception {
        ClusterFixture clusterFixture = ClusterFixture.builder().build();

        dockerCluster = UNIVERSE
                .buildDockerCluster(clusterFixture.data(), docker)
                .deploy();

        ScenarioConfig scenarioCfg = JSON.readValue(
                ClassLoader.getSystemResource("scenario.json"),
                ScenarioConfig.class
        );

        scenarioCfg.getScenario().forEach(test -> {
            test.getAction().cluster = dockerCluster;

            Scenario<ClusterParams, ClusterFixture> scenario = Scenario.with(clusterFixture);
            scenario.describe((fixture, testCase) -> {
                testCase.it(test.getDescription(), Object.class, scenarioTest -> {
                    scenarioTest.action(test.getAction()).check(test.getSpec());
                });
            });
        });
    }
}
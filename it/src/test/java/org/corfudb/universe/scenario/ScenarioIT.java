package org.corfudb.universe.scenario;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import org.corfudb.universe.Universe;
import org.corfudb.universe.cluster.Cluster.ClusterParams;
import org.corfudb.universe.cluster.docker.DockerCluster;
import org.corfudb.universe.scenario.config.ScenarioConfig;
import org.corfudb.universe.scenario.config.ScenarioConfigParser;
import org.corfudb.universe.scenario.fixture.Fixtures.ClusterFixture;
import org.junit.After;
import org.junit.Test;

import java.util.List;

public class ScenarioIT {
    private static final Universe UNIVERSE = Universe.getInstance();


    private DockerCluster dockerCluster;
    private final DockerClient docker;

    public ScenarioIT() throws Exception {
        this.docker = DefaultDockerClient.fromEnv().build();
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

        List<ScenarioConfig> scenarios = ScenarioConfigParser.getInstance().load("scenarios");

        scenarios.forEach(scenarioCfg -> {
            scenarioCfg.getScenario().forEach(test -> {
                test.getAction().cluster = dockerCluster;

                Scenario<ClusterParams, ClusterFixture> scenario = Scenario.with(clusterFixture);
                scenario.describe((fixture, testCase) -> {
                    testCase.it(test.getDescription(), Object.class, scenarioTest -> {
                        scenarioTest.action(test.getAction()).check(test.getSpec());
                    });
                });
            });
        });
    }
}
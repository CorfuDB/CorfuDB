package org.corfudb.universe.scenario;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import org.corfudb.universe.UniverseFactory;
import org.corfudb.universe.scenario.config.ScenarioConfig;
import org.corfudb.universe.scenario.config.ScenarioConfigParser;
import org.corfudb.universe.scenario.fixture.Fixtures.UniverseFixture;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.Universe.UniverseParams;
import org.junit.After;
import org.junit.Test;

import java.util.List;

public class ScenarioIT {
    private static final UniverseFactory UNIVERSE_FACTORY = UniverseFactory.getInstance();


    private Universe dockerUniverse;
    private final DockerClient docker;

    public ScenarioIT() throws Exception {
        this.docker = DefaultDockerClient.fromEnv().build();
    }

    @After
    public void tearDown() {
        if (dockerUniverse != null) {
            dockerUniverse.shutdown();
        }
    }

    @Test
    public void scenarioTest() throws Exception {
        UniverseFixture universeFixture = UniverseFixture.builder().build();

        dockerUniverse = UNIVERSE_FACTORY
                .buildDockerCluster(universeFixture.data(), docker)
                .deploy();

        List<ScenarioConfig> scenarios = ScenarioConfigParser.getInstance().load("scenarios");

        scenarios.forEach(scenarioCfg -> {
            scenarioCfg.getScenario().forEach(test -> {
                test.getAction().universe = dockerUniverse;

                Scenario<UniverseParams, UniverseFixture> scenario = Scenario.with(universeFixture);
                scenario.describe((fixture, testCase) -> {
                    testCase.it(test.getDescription(), Object.class, scenarioTest -> {
                        scenarioTest.action(test.getAction()).check(test.getSpec());
                    });
                });
            });
        });
    }
}
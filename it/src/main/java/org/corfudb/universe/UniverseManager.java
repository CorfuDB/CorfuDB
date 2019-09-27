package org.corfudb.universe;

import com.google.common.collect.ImmutableSet;
import com.spotify.docker.client.DockerClient;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.universe.logging.LoggingParams;
import org.corfudb.universe.scenario.Scenario;
import org.corfudb.universe.scenario.fixture.Fixtures;
import org.corfudb.universe.scenario.fixture.Fixtures.AbstractUniverseFixture;
import org.corfudb.universe.scenario.fixture.Fixtures.UniverseFixture;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.Universe.UniverseMode;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.universe.universe.vm.ApplianceManager;
import org.corfudb.universe.universe.vm.VmUniverseParams;

import java.util.Set;

@Builder
public class UniverseManager {

    private static final UniverseFactory UNIVERSE_FACTORY = UniverseFactory.getInstance();

    @Default
    @Getter
    private final UniverseMode universeMode = UniverseMode.DOCKER;

    @NonNull
    private final String corfuServerVersion;

    @NonNull
    private final DockerClient docker;

    @NonNull
    private final String testName;

    private final boolean enableLogging;

    @Getter
    private Universe universe;

    public void tearDown() {
        if (universe != null) {
            universe.shutdown();
        } else {
            throw new IllegalStateException(
                    "The universe is null, can't shutdown the test properly. " +
                            "Please check docker network leaks"
            );
        }
    }

    public LoggingParams getDockerLoggingParams() {
        return LoggingParams.builder()
                .testName(testName)
                .enabled(enableLogging)
                .build();
    }

    public Scenario getVmScenario(int numNodes) {
        Fixtures.VmUniverseFixture universeFixture = new Fixtures.VmUniverseFixture();
        universeFixture.setNumNodes(numNodes);
        universeFixture.setCorfuServerVersion(corfuServerVersion);

        VmUniverseParams universeParams = universeFixture.data();

        ApplianceManager manager = ApplianceManager.builder()
                .universeParams(universeParams)
                .build();

        //Assign universe variable before deploy prevents resources leaks
        universe = UNIVERSE_FACTORY.buildVmUniverse(universeParams, manager);
        universe.deploy();

        return Scenario.with(universeFixture);
    }

    public Scenario getDockerScenario(int numNodes, Set<Integer> metricsPorts) {
        UniverseFixture universeFixture = new UniverseFixture();
        universeFixture.setNumNodes(numNodes);
        universeFixture.setMetricsPorts(metricsPorts);
        universeFixture.setCorfuServerVersion(corfuServerVersion);

        //Assign universe variable before deploy prevents resources leaks
        universe = UNIVERSE_FACTORY.buildDockerUniverse(
                universeFixture.data(),
                docker,
                getDockerLoggingParams()
        );

        universe.deploy();

        return Scenario.with(universeFixture);
    }

    public Scenario<UniverseParams, AbstractUniverseFixture<UniverseParams>> getScenario() {
        final int defaultNumNodes = 3;
        return getScenario(defaultNumNodes, ImmutableSet.of());
    }

    public Scenario<UniverseParams, AbstractUniverseFixture<UniverseParams>> getScenario(
            int numNodes) {
        return getScenario(numNodes, ImmutableSet.of());
    }

    public Scenario<UniverseParams, AbstractUniverseFixture<UniverseParams>> getScenario(
            int numNodes, Set<Integer> metricsPorts) {

        switch (universeMode) {
            case DOCKER:
                return getDockerScenario(numNodes, metricsPorts);
            case VM:
                return getVmScenario(numNodes);
            case PROCESS:
                throw new UnsupportedOperationException("Not implemented");
            default:
                throw new UnsupportedOperationException("Not implemented");
        }
    }
}

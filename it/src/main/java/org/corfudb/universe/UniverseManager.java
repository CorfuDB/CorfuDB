package org.corfudb.universe;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ClassUtils;
import org.corfudb.universe.logging.LoggingParams;
import org.corfudb.universe.scenario.fixture.Fixture;
import org.corfudb.universe.scenario.fixture.Fixtures.UniverseFixture;
import org.corfudb.universe.scenario.fixture.Fixtures.VmUniverseFixture;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.Universe.UniverseMode;
import org.corfudb.universe.universe.UniverseException;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.universe.universe.docker.DockerUniverse;
import org.corfudb.universe.universe.process.ProcessUniverse;
import org.corfudb.universe.universe.vm.ApplianceManager;
import org.corfudb.universe.universe.vm.VmUniverse;
import org.corfudb.universe.universe.vm.VmUniverseParams;

import java.util.function.Consumer;

/**
 * Manages UniverseWorkflow and provides api to build a universe workflow.
 */
@Builder
public class UniverseManager {

    @NonNull
    private final String corfuServerVersion;

    @NonNull
    private final String testName;

    @Default
    @Getter
    private final UniverseMode universeMode = UniverseMode.DOCKER;

    /**
     * Creates a universe workflow.
     *
     * @param action testing logic
     * @param <T>    a fixture type
     * @return universe workflow
     */
    public <T extends Fixture<UniverseParams>> UniverseWorkflow workflow(
            Consumer<UniverseWorkflow<T>> action) {

        UniverseWorkflow<T> wf = workflow();
        try {
            action.accept(wf);
        } finally {
            wf.shutdown();
        }

        return wf;
    }

    private <T extends Fixture<UniverseParams>> UniverseWorkflow<T> workflow() {

        T fixture;
        switch (universeMode) {
            case DOCKER:
            case PROCESS:
                fixture = ClassUtils.cast(new UniverseFixture());
                break;
            case VM:
                fixture = ClassUtils.cast(new VmUniverseFixture());
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + universeMode);
        }

        return UniverseWorkflow.<T>builder()
                .corfuServerVersion(corfuServerVersion)
                .universeMode(universeMode)
                .testName(testName)
                .fixture(fixture)
                .build()
                .init();
    }

    /**
     * Manages a testing workflow lifecycle.
     * Provides api to:
     * - customize the universe parameters
     * - initialize and deploy the clusters in the universe
     * - get the universe fixture to setup the initial parameters
     * - shutdown the universe
     *
     * @param <T>
     */
    @Slf4j
    @Builder
    @Getter
    public static class UniverseWorkflow<T extends Fixture<UniverseParams>> {
        @NonNull
        private final T fixture;
        @NonNull
        private final UniverseMode universeMode;
        @NonNull
        private String corfuServerVersion;

        @NonNull
        private final String testName;

        @Getter
        private Universe universe;

        private boolean initialized;

        public UniverseWorkflow<T> init() {
            switch (universeMode) {
                case DOCKER:
                case PROCESS:
                    UniverseFixture dockerFixture = ClassUtils.cast(this.fixture);
                    dockerFixture.getCluster().serverVersion(corfuServerVersion);
                    break;
                case VM:
                    VmUniverseFixture vmFixture = ClassUtils.cast(fixture);
                    vmFixture.getCluster().serverVersion(corfuServerVersion);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + universeMode);
            }

            return this;
        }

        public UniverseWorkflow<T> initUniverse() {
            switch (universeMode) {
                case DOCKER:
                    initDockerUniverse();
                    break;
                case VM:
                    initVmUniverse();
                    break;
                case PROCESS:
                    initProcessUniverse();
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + universeMode);
            }

            initialized = true;

            return this;
        }

        public UniverseWorkflow<T> deploy() {
            initUniverse();

            switch (universeMode) {
                case DOCKER:
                    deployDocker();
                    break;
                case VM:
                    deployVm();
                    break;
                case PROCESS:
                    deployProcess();
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + universeMode);
            }

            return this;
        }

        public UniverseWorkflow<T> setupDocker(Consumer<UniverseFixture> fixtureManager) {
            if (universeMode == UniverseMode.DOCKER) {
                fixtureManager.accept(ClassUtils.cast(fixture));
            }

            return this;
        }

        public UniverseWorkflow<T> setupVm(Consumer<VmUniverseFixture> fixtureManager) {
            if (universeMode == UniverseMode.VM) {
                fixtureManager.accept(ClassUtils.cast(fixture));
            }

            return this;
        }

        public UniverseWorkflow<T> setupProcess(Consumer<UniverseFixture> fixtureManager) {
            if (universeMode == UniverseMode.PROCESS) {
                fixtureManager.accept(ClassUtils.cast(fixture));
            }

            return this;
        }

        private UniverseWorkflow<T> deployVm() {
            checkMode(UniverseMode.VM);

            universe.deploy();

            return this;
        }

        private UniverseWorkflow<T> initVmUniverse() {
            checkMode(UniverseMode.VM);

            if (initialized) {
                return this;
            }

            VmUniverseParams universeParams = ClassUtils.cast(fixture.data());

            ApplianceManager manager = ApplianceManager.builder()
                    .universeParams(universeParams)
                    .build();

            VmUniverseFixture vmFixture = ClassUtils.cast(fixture);
            LoggingParams loggingParams = vmFixture
                    .getLogging()
                    .testName(testName)
                    .build();

            //Assign universe variable before deploy prevents resources leaks
            universe = VmUniverse.builder()
                    .universeParams(universeParams)
                    .loggingParams(loggingParams)
                    .applianceManager(manager)
                    .build();

            return this;
        }

        private UniverseWorkflow<T> deployDocker() {
            //Assign universe variable before deploy stage, it prevents resources leaks.
            checkMode(UniverseMode.DOCKER);

            universe.deploy();

            return this;
        }

        private UniverseWorkflow<T> initDockerUniverse() {
            checkMode(UniverseMode.DOCKER);

            if (initialized) {
                return this;
            }

            DefaultDockerClient docker;
            try {
                docker = DefaultDockerClient.fromEnv().build();
            } catch (DockerCertificateException e) {
                throw new UniverseException("Can't initialize docker client");
            }

            UniverseFixture dockerFixture = ClassUtils.cast(fixture);
            LoggingParams loggingParams = dockerFixture
                    .getLogging()
                    .testName(testName)
                    .build();

            universe = DockerUniverse.builder()
                    .universeParams(dockerFixture.data())
                    .loggingParams(loggingParams)
                    .docker(docker)
                    .build();

            return this;
        }

        private UniverseWorkflow<T> deployProcess() {
            checkMode(UniverseMode.PROCESS);

            universe.deploy();

            return this;
        }

        private UniverseWorkflow<T> initProcessUniverse() {
            checkMode(UniverseMode.PROCESS);

            if (initialized) {
                return this;
            }

            UniverseFixture processFixture = ClassUtils.cast(fixture);
            LoggingParams loggingParams = processFixture
                    .getLogging()
                    .testName(testName)
                    .build();

            //Assign universe variable before deploy prevents resources leaks
            universe = ProcessUniverse.builder()
                    .universeParams(fixture.data())
                    .loggingParams(loggingParams)
                    .build();

            return this;
        }

        public void shutdown() {
            if (universe != null) {
                universe.shutdown();
            } else {
                String err = "The universe is null, can't shutdown the test properly.";
                log.error(err);
            }
        }

        private void checkMode(UniverseMode expected) {
            if (!universeMode.equals(expected)) {
                String err = "Invalid mode: " + universeMode + ". Expected mode: " + expected;
                throw new IllegalStateException(err);
            }
        }
    }
}

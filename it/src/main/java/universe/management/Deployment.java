package universe.management;

import lombok.extern.slf4j.Slf4j;
import universe.UniverseConfigurator;

import java.util.Optional;

@Slf4j
public class Deployment {

    private final UniverseConfigurator configurator = UniverseConfigurator.builder().build();

    /**
     * Deploying a corfu cluster:
     * - disable shutdown logic to prevent the universe stop and clean corfu servers
     * - deploy corfu server
     */
    public static void main(String[] args) {
        log.info("Deploying corfu cluster...");

        Deployment deployment = new Deployment();
        deployment.deploy();

        log.info("Corfu cluster has deployed");
    }

    public Deployment deploy() {
        configurator.universeManager.workflow(wf -> {
            wf.setupProcess(fixtura -> {
                fixtura.getCluster().name("ST");
                fixtura.getFixtureUtilBuilder().initialPort(Optional.of(9000));
                fixtura.getUniverse().cleanUpEnabled(false);
            });
            //disable shutdown logic
            wf.deploy();
        });

        return this;
    }
}

package universe.management;

import lombok.extern.slf4j.Slf4j;
import universe.UniverseConfigurator;

import java.util.Optional;

@Slf4j
public class Shutdown {

    private final UniverseConfigurator configurator = UniverseConfigurator.builder().build();

    /**
     * Shutdown corfu cluster: stop corfu processes and delete corfu directory.
     *
     * @param args args
     */
    public static void main(String[] args) {
        log.info("Corfu cluster is being shutdown...");
        new Shutdown().shutdown();
        log.info("Corfu cluster shutdown has finished ");
    }

    public Shutdown shutdown() {
        configurator.processUniverseManager.workflow(wf -> {
            wf.setupProcess(fixtura -> {
                fixtura.getCluster().name("ST");
                fixtura.getFixtureUtilBuilder().initialPort(Optional.of(9000));
            });
            wf.deploy();
            wf.shutdown();
        });

        return this;
    }
}

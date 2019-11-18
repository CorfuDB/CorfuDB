package universe;

import lombok.Builder;
import lombok.Builder.Default;
import org.corfudb.universe.UniverseManager;
import org.corfudb.universe.scenario.fixture.Fixtures.UniverseFixture;
import org.corfudb.universe.scenario.fixture.Fixtures.VmUniverseFixture;
import org.corfudb.universe.universe.Universe.UniverseMode;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Consumer;

@Builder
public class UniverseConfigurator {

    private static final String TEST_NAME = "corfu_qa";
    private static final String SERVER_VERSION = "0.3.0-SNAPSHOT";

    @Default
    public final UniverseManager universeManager = UniverseManager.builder()
            .testName(TEST_NAME)
            .universeMode(UniverseMode.VM)
            .corfuServerVersion(SERVER_VERSION)
            .build();

    @Default
    public final UniverseManager dockerUniverseManager = UniverseManager.builder()
            .testName(TEST_NAME)
            .universeMode(UniverseMode.DOCKER)
            .corfuServerVersion(SERVER_VERSION)
            .build();

    @Default
    public final UniverseManager processUniverseManager = UniverseManager.builder()
            .testName(TEST_NAME)
            .universeMode(UniverseMode.PROCESS)
            .corfuServerVersion(SERVER_VERSION)
            .build();

    @Default
    public final Consumer<VmUniverseFixture> vmSetup = fixture -> {
        fixture.setVmPrefix("corfu-vm-dynamic-qe");
        fixture.getCluster().name("static_cluster");
        fixture.getFixtureUtilBuilder().initialPort(Optional.of(9000));

        fixture.getUniverse()
                .vSphereUrl("https://10.173.65.98/sdk")
                .vSphereHost(Arrays.asList("10.172.208.208"))
                .templateVMName("corfu-server");
    };

    @Default
    public final Consumer<UniverseFixture> dockerSetup = fixture -> {
        fixture.getServer().dockerImage("corfudb/corfu-server");
    };
}

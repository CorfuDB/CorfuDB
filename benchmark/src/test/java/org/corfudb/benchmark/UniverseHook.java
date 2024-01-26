package org.corfudb.benchmark;

import org.corfudb.common.util.ClassUtils;
import org.corfudb.universe.UniverseAppUtil;
import org.corfudb.universe.UniverseManager;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.scenario.fixture.Fixtures;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.UniverseParams;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import java.util.TimeZone;

@State(Scope.Benchmark)
public class UniverseHook {

    public static final String DEFAULT_STREAM_NAMESPACE = "namespace";
    public static final String DEFAULT_STREAM_NAME = "stream";
    protected static final UniverseAppUtil APP_UTIL = new UniverseAppUtil();

    protected UniverseManager universeManager;

    protected void setupUniverseFramework(UniverseBenchmarkState state) {
        state.universeManager = UniverseManager.builder()
                .testName(this.getClass().getSimpleName())
                .universeMode(Universe.UniverseMode.DOCKER)
                .corfuServerVersion(APP_UTIL.getAppVersion())
                .build();

        state.wf = state.universeManager.workflow();
        Fixtures.UniverseFixture dockerFixture = ClassUtils.cast(state.wf.getFixture());
        dockerFixture.getServer().containerResources(
                CorfuServerParams.ContainerResources.builder().memory(Optional.of(1024L)).build());
        state.wf.deploy();

        UniverseParams params = state.wf.getFixture().data();

        state.corfuCluster = state.wf.getUniverse()
                .getGroup(params.getGroupParamByIndex(0).getName());
        state.corfuClient = state.corfuCluster.getLocalCorfuClient();
    }

    protected static ChainedOptionsBuilder jmhBuilder() {
        String className = Thread.currentThread().getStackTrace()[2].getClassName();
        SimpleDateFormat dateFormat = new SimpleDateFormat( "yyyy-MM-dd-HH-mm-ss" );
        dateFormat.setTimeZone( TimeZone.getTimeZone( "UTC" ) );
        String dateTime = dateFormat.format(new Date());;
        return new OptionsBuilder()
                .include(className)
                .resultFormat(ResultFormatType.JSON)
                .result("benchmark-results/" + className + "-" + dateTime + ".json")
                .addProfiler("gc")
                .shouldDoGC(true)
                .jvmArgs("-Xms1G", "-Xmx1G")
                .forks(1);
    }
}

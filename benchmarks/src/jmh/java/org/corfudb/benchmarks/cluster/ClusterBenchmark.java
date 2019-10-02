package org.corfudb.benchmarks.cluster;

import static org.corfudb.benchmarks.util.DataUnit.KB;
import static org.corfudb.benchmarks.util.DataUnit.MB;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.corfudb.benchmarks.util.DataGenerator;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.universe.UniverseManager;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.universe.Universe.UniverseMode;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@Slf4j
public class ClusterBenchmark {

    public static void main(String[] args) throws RunnerException {
        String benchmarkName = ClusterBenchmark.class.getSimpleName();

        int warmUpIterations = 0;

        int measurementIterations = 1;
        TimeValue measurementTime = TimeValue.seconds(60);

        int threads = 4;
        int forks = 1;

        String[] dataSizes = Stream
                .of(KB.toBytes(4), MB.toBytes(2))
                .map(String::valueOf)
                .toArray(String[]::new);

        String[] numRuntime = {"1", "4", "8"};
        String[] numTables = {"1", "4", "8"};

        String[] numServers = {"1", "3"};

        Options opt = new OptionsBuilder()
                .include(benchmarkName)

                .mode(Mode.Throughput)
                .timeUnit(TimeUnit.SECONDS)

                .warmupIterations(warmUpIterations)

                .measurementIterations(measurementIterations)
                .measurementTime(measurementTime)

                .param("dataSize", dataSizes)
                .param("numRuntime", numRuntime)
                .param("numTables", numTables)
                .param("numServers", numServers)

                .threads(threads)
                .forks(forks)

                .shouldFailOnError(true)

                .resultFormat(ResultFormatType.CSV)
                .result("target/" + benchmarkName + ".csv")

                .build();

        new Runner(opt).run();
    }

    @State(Scope.Benchmark)
    @Getter
    @Slf4j
    public static class ClusterBenchmarkState {

        public final Random rnd = new Random();

        private UniverseManager universeManager;

        @Param({})
        private int dataSize;

        @Param({})
        public int numRuntime;

        @Param({})
        public int numTables;

        @Param({})
        public int numServers;

        private String data;

        public final List<CorfuClient> corfuClients = new ArrayList<>();
        public final List<CorfuTable<String, String>> tables = new ArrayList<>();
        public final AtomicInteger counter = new AtomicInteger(1);

        @Setup
        public void init() throws DockerCertificateException {

            data = DataGenerator.generateDataString(dataSize);

            universeManager = UniverseManager.builder()
                    .docker(DefaultDockerClient.fromEnv().build())
                    .enableLogging(false)
                    .testName("corfu_cluster_benchmark")
                    .universeMode(UniverseMode.PROCESS)
                    .corfuServerVersion(getAppVersion())
                    .build();

            universeManager.getScenario(numServers).describe((fixture, testCase) -> {
                CorfuCluster corfuCluster = universeManager
                        .getUniverse()
                        .getGroup(fixture.getCorfuCluster().getName());

                for (int i = 0; i < numRuntime; i++) {
                    corfuClients.add(corfuCluster.getLocalCorfuClient());
                }

                for (int i = 0; i < numTables; i++) {
                    CorfuClient corfuClient = getRandomCorfuClient();
                    CorfuTable<String, String> table = corfuClient
                            .createDefaultCorfuTable(DEFAULT_STREAM_NAME + i);
                    tables.add(table);
                }
            });
        }

        public CorfuClient getRandomCorfuClient() {
            return corfuClients.get(rnd.nextInt(numRuntime));
        }

        public CorfuTable<String, String> getRandomTable() {
            return tables.get(rnd.nextInt(numTables));
        }

        @TearDown
        public void tearDown() {

            for (CorfuClient corfuClient : corfuClients) {
                corfuClient.shutdown();
            }
            universeManager.tearDown();
        }

        /**
         * Provides a current version of this project. It parses the version from pom.xml
         *
         * @return maven/project version
         */
        private String getAppVersion() {
            return parseAppVersionInPom();
        }

        private String parseAppVersionInPom() {
            MavenXpp3Reader reader = new MavenXpp3Reader();
            Model model;
            try {
                model = reader.read(new FileReader("pom.xml"));
                return model.getVersion();
            } catch (IOException | XmlPullParserException e) {
                throw new IllegalStateException("Can't parse application version", e);
            }
        }
    }

    @Benchmark
    public void clusterBenchmark(ClusterBenchmarkState state) {
        String key = String.valueOf(state.counter.getAndIncrement());
        CorfuTable<String, String> table = state.getRandomTable();
        table.put(key, state.data);
    }
}

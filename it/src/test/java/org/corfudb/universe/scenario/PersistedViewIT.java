package org.corfudb.universe.scenario;

import com.google.common.reflect.TypeToken;
import org.apache.commons.lang.RandomStringUtils;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.collections.PersistedCorfuTable;
import org.corfudb.runtime.collections.PersistedStreamingMap;
import org.corfudb.runtime.collections.StreamingMap;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.UniverseManager.UniverseWorkflow;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.scenario.fixture.Fixture;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(0)
@State(Scope.Benchmark)
public class PersistedViewIT extends GenericIntegrationTest {

    public static UniverseWorkflow<Fixture<UniverseParams>> wf;
    public static ICorfuTable<String, String> table;
    public static ICorfuTable<String, String> persistedTable;
    public static CorfuClient corfuClient;

    private final long SAMPLE_SIZE = 2000;
    private final int VALUE_LEN = 5_000;
    private final int MAP_SIZE = 5_000;

    @Test(timeout = Integer.MAX_VALUE)
    public void JHM() {

        workflow(wf -> {
            wf.deploy();
            PersistedViewIT.wf = wf;
            setup();

            org.openjdk.jmh.runner.options.Options opt = new OptionsBuilder()
                    .include(PersistedViewIT.class.getSimpleName())
                    .warmupIterations(1)
                    .measurementIterations(2)
                    .build();

            try {
                new Runner(opt).run();
            } catch (RunnerException e) {
                e.printStackTrace();
                System.exit(-1);
            }

        });
    }

    public void populate() {
        for (int i = 0; i < MAP_SIZE; i++) {
            String value = i + RandomStringUtils.random(VALUE_LEN);
            corfuClient.getRuntime().getObjectsView().TXBegin();
            table.put(String.valueOf(i), value);
            corfuClient.getRuntime().getObjectsView().TXEnd();
            persistedTable.put(String.valueOf(i), value);
            if (i % 100 == 0) System.out.println("Priming the data... " + i);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void scanAddressSpace() {
        final CorfuRuntime corfuRuntime = corfuClient.getRuntime();
        Set<Long> addresses = LongStream.range(0, MAP_SIZE)
                .boxed().collect(Collectors.toSet());

        Instant start = Instant.now();
        Map<Long, ILogData> result = corfuRuntime.getAddressSpaceView().read(addresses);
        Instant finish = Instant.now();
        System.out.println("Log Unit Scan: " + Duration.between(start, finish).toMillis() + "ms |  " + addresses.size());
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void scanCorfuTable() {
        final CorfuRuntime corfuRuntime = corfuClient.getRuntime();
        Set<Long> addresses = (new Random().longs(SAMPLE_SIZE, 0, MAP_SIZE)
                .boxed().collect(Collectors.toSet()));

        Instant start = Instant.now();
        corfuClient.getRuntime().getObjectsView().executeTx(() -> {
            table.entryStream().forEach(entry -> {});
        });
        Instant finish = Instant.now();
        System.out.println("Direct Scan: " + Duration.between(start, finish).toMillis() + "ms |  " + addresses.size());
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void scanPersistedObjectsView() {
        final CorfuRuntime corfuRuntime = corfuClient.getRuntime();
        Set<Long> addresses = (new Random().longs(SAMPLE_SIZE, 0, MAP_SIZE)
                .boxed().collect(Collectors.toSet()));

        Instant start = Instant.now();
        corfuClient.getRuntime().getObjectsView().executeTx(() -> {
            persistedTable.entryStream().forEach(entry -> {});
        });
        Instant finish = Instant.now();
        System.out.println("Persisted Scan: " + Duration.between(start, finish).toMillis() + "ms |  " + addresses.size());
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void pinpointReadAddressSpace() {
        final CorfuRuntime corfuRuntime = corfuClient.getRuntime();
        Set<Long> addresses = (new Random().longs(SAMPLE_SIZE, 0, MAP_SIZE)
                .boxed().collect(Collectors.toSet()));

        Instant start = Instant.now();
        addresses.forEach(address -> corfuRuntime.getAddressSpaceView().read(address));
        Instant finish = Instant.now();
        System.out.println("Log Unit Pin Point: " + Duration.between(start, finish).toMillis() + "ms |  " + addresses.size());
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void pinpointReadCorfuTable() {
        Set<Long> addresses = (new Random().longs(SAMPLE_SIZE, 0, MAP_SIZE)
                .boxed().collect(Collectors.toSet()));

        Instant start = Instant.now();
        corfuClient.getRuntime().getObjectsView().executeTx(() -> {
            Set<String> values = addresses.stream()
                    .map(idx -> table.get(String.valueOf(idx)))
                    .collect(Collectors.toSet());
        });

        Instant finish = Instant.now();
        System.out.println("Direct Pin Point: " + Duration.between(start, finish).toMillis() + "ms |  " + addresses.size());
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void pinpointReadPersisted() {
        Set<Long> addresses = (new Random().longs(SAMPLE_SIZE, 0, MAP_SIZE)
                .boxed().collect(Collectors.toSet()));

        Instant start = Instant.now();
        corfuClient.getRuntime().getObjectsView().executeTx(() -> {
            Set<String> values = addresses.stream()
                    .map(idx -> persistedTable.get(String.valueOf(idx)))
                    .collect(Collectors.toSet());
        });

        Instant finish = Instant.now();
        System.out.println("Persisted Pin Point: " + Duration.between(start, finish).toMillis() + "ms |  " + addresses.size());
    }

    public void setup() {
        String groupName = wf.getFixture().data().getGroupParamByIndex(0).getName();
        CorfuCluster corfuCluster = wf.getUniverse().getGroup(groupName);
        corfuClient = corfuCluster.getLocalCorfuClient(
                CorfuRuntime.CorfuRuntimeParameters.builder().cacheDisabled(true));

        final String tableName = "diskBackedMap";
        Options options = new Options();
        options.setCreateIfMissing(true);
        options.setCompressionType(CompressionType.NO_COMPRESSION);
        final Path persistedCacheLocation = Paths.get("/tmp/", "myData");
        ICorfuVersionPolicy.VersionPolicy versionPolicy = ICorfuVersionPolicy.DEFAULT;

        Supplier<StreamingMap<String, String>> mapSupplier = () -> new PersistedStreamingMap<>(
                persistedCacheLocation,
                PersistedStreamingMap.getPersistedStreamingMapOptions(),
                Serializers.JSON, corfuClient.getRuntime());
        table = corfuClient.getRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setArguments(mapSupplier, versionPolicy)
                .setStreamName(tableName)
                .open();
        persistedTable = new PersistedCorfuTable<>(corfuClient.getRuntime(),
                tableName + "Persisted");

        populate();
    }
}

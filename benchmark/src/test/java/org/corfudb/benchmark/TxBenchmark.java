package org.corfudb.benchmark;

import org.apache.commons.lang3.RandomStringUtils;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.universe.UniverseManager;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.scenario.fixture.Fixtures;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.benchmark.Schema;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class TxBenchmark extends GenericIntegrationTest {

    private int payloadSize = 10;
    private int maxTxCount = 256;
    private int maxPutCount = 64;

    private static final int minThreadCount = 1;
    private static final int maxThreadCount = 32;

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        Table<Schema.Uuid, Schema.StringValue, ?> table;
        CorfuStore store;
        CorfuClient corfuClient;

        CorfuCluster corfuCluster;

        UniverseManager.UniverseWorkflow<?> workflow;
    }


    public static void main(String[] args) throws RunnerException {
        for (int threadCount = minThreadCount; threadCount <= maxThreadCount; threadCount = threadCount * 2) {
            Options opt = new OptionsBuilder()
                    .include(TxBenchmark.class.getSimpleName())
                    //.verbosity(SILENT)
                    .addProfiler("gc")
                    .forks(1)
                    .jvmArgs("-Xms1G", "-Xmx1G"
                            //,"-agentpath:/home/vjeko/Downloads/YourKit-JavaProfiler-2022.3/bin/linux-x86-64/libyjpagent.so=onexit=snapshot"
                            )
                    .threads(threadCount)
                    .build();

            System.out.println("Thread Count: " + threadCount);
            Collection<RunResult> results = new Runner(opt).run();
            for (RunResult result : results) {
                System.out.println(result.getPrimaryResult().toString());
                final Result primary = result.getPrimaryResult();
                System.out.println(primary.getLabel() + ": " + primary.getScore() + " +/-" +
                        primary.getScoreError() + " " + primary.getScoreUnit());
            }
        }
    }

    @Setup
    public void setup(BenchmarkState state) throws
            InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        universeManager = UniverseManager.builder()
                .testName(TxLongBenchmark.class.getSimpleName())
                .universeMode(Universe.UniverseMode.DOCKER)
                .corfuServerVersion(APP_UTIL.getAppVersion())
                .build();
        state.workflow = universeManager.workflow();
        state.workflow.deploy();

        UniverseParams params = state.workflow.getFixture().data();

        state.corfuCluster = state.workflow.getUniverse()
                .getGroup(params.getGroupParamByIndex(0).getName());
        state.corfuClient = state.corfuCluster.getLocalCorfuClient();
        state.store = new CorfuStore(state.corfuCluster.getLocalCorfuClient().getRuntime());
        state.table = openTable(state.store);

    }

    private Table<Schema.Uuid, Schema.StringValue, ?> openTable(CorfuStore store) throws
            InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        return store.openTable(
                Fixtures.TestFixtureConst.DEFAULT_STREAM_NAMESPACE,
                Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME,
                Schema.Uuid.class,
                Schema.StringValue.class,
                null,
                TableOptions.builder().build());
    }

    @TearDown
    public void tearDown(BenchmarkState state) {
        state.corfuClient.shutdown();
        state.workflow.getUniverse().shutdown();
    }


    @Benchmark
    @Measurement(iterations = 1)
    @Warmup(iterations = 1)
    public void singleThreadedWrite(BenchmarkState state) {
        Random random = new Random();
        for (int txCount = 0; txCount < maxTxCount; txCount++) {
            try {
                TxnContext tx = state.store.txn(Fixtures.TestFixtureConst.DEFAULT_STREAM_NAMESPACE);

                for (int putCount = 0; putCount < maxPutCount; putCount++) {
                    Schema.Uuid key = Schema.Uuid.newBuilder().setLsb(random.nextLong())
                            .setMsb(random.nextLong()).build();
                    final String payload = RandomStringUtils.random(payloadSize);
                    Schema.StringValue value = Schema.StringValue.newBuilder()
                            .setValue(payload)
                            .setSecondary(payload)
                            .build();
                    tx.getRecord(state.table, key);
                    tx.putRecord(state.table, key, value, null);
                }

                tx.commit();
            } catch(TransactionAbortedException ex){
                if (ex.getAbortCause() != AbortCause.CONFLICT) {
                    throw ex;
                }
            }
        }
    }

}

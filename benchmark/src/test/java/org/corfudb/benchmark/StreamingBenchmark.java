package org.corfudb.benchmark;

import lombok.Getter;
import org.apache.commons.lang3.RandomStringUtils;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.universe.UniverseManager;
import org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.UniverseParams;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class StreamingBenchmark extends UniverseHook {

    private final int payloadSize = 10;
    private final int maxTxCount = 16;
    private final int batchSize = 8;
    private final int maxPutCount = 4;

    private static final int minThreadCount = 1;
    private static final int maxThreadCount = 32;

    @State(Scope.Benchmark)
    public static class BenchmarkState extends UniverseBenchmarkState{
        CorfuStore store;
        Table<Schema.Uuid, Schema.StringStreamingValue, ?> table;
        Timestamp timestamp;
    }

    private static class StreamListenerImpl implements StreamListener {

        @Getter
        private final String name;

        @Getter
        private Timestamp timestamp;

        @Getter
        private final LinkedList<CorfuStreamEntries> updates = new LinkedList<>();

        private final CountDownLatch updateLatch;

        StreamListenerImpl(String name, CountDownLatch updateLatch) {
            this.name = name;
            this.updateLatch = updateLatch;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            updates.add(results);
            timestamp = results.getTimestamp();
            updateLatch.countDown();
        }

        @Override
        public void onError(Throwable throwable) {

        }

        @Override
        public String toString() {
            return name;
        }
    }

    public static void main(String[] args) throws RunnerException {
        for (int threadCount = minThreadCount; threadCount <= maxThreadCount; threadCount = threadCount * 2) {
            Options opt = jmhBuilder()
                    .threads(threadCount)
                    .build();

            Collection<RunResult> results = new Runner(opt).run();
        }
    }

    @Setup(Level.Iteration)
    public void prepare(BenchmarkState state) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        state.store = new CorfuStore(state.corfuCluster.getLocalCorfuClient().getRuntime());
        state.table = openTable(state.store);

        state.timestamp = getTimestamp(state.corfuClient.getRuntime());
    }

    @Setup
    public void setup(BenchmarkState state) {
        universeManager = UniverseManager.builder()
                .testName(TxLongBenchmark.class.getSimpleName())
                .universeMode(Universe.UniverseMode.DOCKER)
                .corfuServerVersion(APP_UTIL.getAppVersion())
                .build();
        state.wf = universeManager.workflow();
        state.wf.deploy();

        UniverseParams params = state.wf.getFixture().data();

        state.corfuCluster = state.wf.getUniverse()
                .getGroup(params.getGroupParamByIndex(0).getName());
        state.corfuClient = state.corfuCluster.getLocalCorfuClient();
    }

    private Table<Schema.Uuid, Schema.StringStreamingValue, ?> openTable(CorfuStore store) throws
            InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        return store.openTable(
                DEFAULT_STREAM_NAMESPACE,
                DEFAULT_STREAM_NAME,
                Schema.Uuid.class,
                Schema.StringStreamingValue.class,
                null,
                TableOptions.fromProtoSchema(Schema.StringStreamingValue.class));
    }

    @TearDown
    public void tearDown(BenchmarkState state) {
        state.corfuClient.shutdown();
        state.wf.getUniverse().shutdown();
    }

    private Timestamp getTimestamp(CorfuRuntime runtime) {
        Token token = runtime.getSequencerView().query().getToken();
        return Timestamp.newBuilder()
                .setEpoch(token.getEpoch())
                .setSequence(token.getSequence())
                .build();
    }

    @Benchmark
    @Measurement(iterations = 1)
    @Warmup(iterations = 1)
    public void subscribeListener(BenchmarkState state, BenchmarkParams params)
            throws InterruptedException {
        System.out.println("Latch set to " + (maxTxCount * params.getThreads()));
        Timestamp currentTimestamp = state.timestamp;
        Timestamp lastTimestamp = currentTimestamp;

        for (int batchCount = 0; batchCount < batchSize; batchCount++) {
            for (int txCount = 0; txCount < maxTxCount; txCount++) {
                TxnContext tx = state.store.txn(DEFAULT_STREAM_NAMESPACE);

                for (int putCount = 0; putCount < maxPutCount; putCount++) {
                    Schema.Uuid key = Schema.Uuid.newBuilder()
                            .setLsb(Thread.currentThread().getId())
                            .setMsb(putCount).build();
                    final String payload = RandomStringUtils.random(payloadSize);
                    Schema.StringStreamingValue value = Schema.StringStreamingValue.newBuilder()
                            .setValue(payload)
                            .setSecondary(payload)
                            .build();
                    tx.getRecord(state.table, key);
                    tx.putRecord(state.table, key, value, null);
                }

                lastTimestamp = tx.commit();
            }

            CountDownLatch latch = new CountDownLatch(maxTxCount * params.getThreads());
            StreamListenerImpl listener = new StreamListenerImpl("stream_listener", latch);
            state.store.subscribeListener(listener, DEFAULT_STREAM_NAMESPACE,
                    "streaming_tag", Collections.singletonList(TestFixtureConst.DEFAULT_STREAM_NAME),
                    currentTimestamp);
            latch.await();
            currentTimestamp = lastTimestamp;
        }
    }

}

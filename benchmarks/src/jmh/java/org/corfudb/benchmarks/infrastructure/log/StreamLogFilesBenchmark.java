package org.corfudb.benchmarks.infrastructure.log;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.io.FileUtils;
import org.corfudb.benchmarks.util.DataGenerator;
import org.corfudb.infrastructure.datastore.DataStore;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.StreamLogDataStore;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.util.serializer.Serializers;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class StreamLogFilesBenchmark {

    public static void main(String[] args) throws RunnerException {

        String benchmarkName = StreamLogFilesBenchmark.class.getSimpleName();

        int warmUpIterations = 1;
        TimeValue warmUpTime = TimeValue.seconds(3);

        int measurementIterations = 3;
        TimeValue measurementTime = TimeValue.seconds(10);

        int threads = 1;
        int forks = 1;

        Options opt = new OptionsBuilder()
                .include(benchmarkName)

                .mode(Mode.Throughput)
                .timeUnit(TimeUnit.SECONDS)

                .warmupIterations(warmUpIterations)
                .warmupTime(warmUpTime)

                .measurementIterations(measurementIterations)
                .measurementTime(measurementTime)

                .threads(threads)
                .forks(forks)

                //kilobytes
                .param("dataSize", "2", "8", "32", "128", "512")

                .shouldFailOnError(true)

                .resultFormat(ResultFormatType.CSV)
                .result("target/" + benchmarkName + ".csv")

                .build();

        new Runner(opt).run();
    }

    @State(Scope.Thread)
    public static class StreamLogState {
        private final Consumer<String> cleanupTask = path -> {
        };

        private final Path logDir = Paths.get("target", "db", "log");

        private final double logSizeLimitPercentage = 100D;

        @Param({})
        private int dataSize;

        private LogData getEntry(long address) {
            ByteBuf buf = Unpooled.buffer();
            byte[] streamEntry = DataGenerator.generateDataString(dataSize * 1024).getBytes();
            Serializers.CORFU.serialize(streamEntry, buf);
            LogData ld = new LogData(DataType.DATA, buf);
            ld.setGlobalAddress(address);
            return ld;
        }

        @Setup
        public void setup() throws IOException {
            FileUtils.deleteDirectory(logDir.toFile());
            FileUtils.forceMkdir(logDir.toFile());

            StreamLog streamLog = buildStreamLog();

            for (int i = 0; i < StreamLogFiles.RECORDS_PER_LOG_FILE; i++) {
                LogData logData = getEntry(i);
                streamLog.append(i, logData);
            }

        }

        @TearDown
        public void tearDown() throws IOException {
            FileUtils.deleteDirectory(logDir.toFile());
        }

        private StreamLog buildStreamLog() {
            return new StreamLogFiles(logDir, buildDataStore(), logSizeLimitPercentage, true);
        }

        private StreamLogDataStore buildDataStore() {
            return StreamLogDataStore.builder()
                    .dataStore(new DataStore(new HashMap<>(), cleanupTask))
                    .build();
        }
    }

    /**
     * StreamLog initialization benchmark. Measures time needed to load one segment from disk.
     * @param state benchmark state
     * @param blackhole blackhole
     */
    @Benchmark
    @OperationsPerInvocation(StreamLogFiles.RECORDS_PER_LOG_FILE)
    public void streamLogInit(StreamLogState state, Blackhole blackhole) {
        StreamLog streamLog = state.buildStreamLog();
        blackhole.consume(streamLog);
    }
}

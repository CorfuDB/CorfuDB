package org.corfudb.benchmarks;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.extern.slf4j.Slf4j;

import org.HdrHistogram.Recorder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.MetricsUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * This class is the super class for all performance tests.
 * It set test parameters like how many clients to run, how many threads each client run,
 * and how many request one thread send.
 * It also set up runtimes, threads, reporter and setup CSV reporter to store csv metrics files
 * to JVM given directory.
 */
@Slf4j
public class PerformanceTest {
    private static class Args {
        @Parameter(names = {"-h", "--help"}, description = "Help message", help = true)
        boolean help;

        @Parameter(names = {"--endpoint"}, description = "Cluster endpoint", required = true)
        String endpoint; //ip:portnum

        @Parameter(names = {"--num-clients"}, description = "Number of clients", required = true)
        int numClients;

        @Parameter(names = {"--num-threads"}, description = "Total number of threads", required = true)
        int numThreads;

        @Parameter(names = {"--num-requests"}, description = "Number of requests per thread", required = true)
        int numRequests;
    }
    /**
     * Number of clients
     */
    protected int numRuntimes = -1;
    /**
     * Number of threads per client.
     */
    protected int numThreads = -1;
    /**
     * Number of requests per thread.
     */
    protected int numRequests = -1;
    /**
     * Server endpoint.
     */
    protected String endpoint = null;
    /**
     * Counter for checking total request number.
     */
    protected LongAdder requestsCompleted = null;
    /**
     * recorder for the test.
     */
    protected Recorder recorder = null;

    protected ExecutorService service = null;
    /**
     * trace for each thread.
     */
    protected SimpleTrace[] traces = null;
    protected CorfuRuntime[] rts = null;

    public PerformanceTest(String[] args) {
        setArgs(args);
        rts = new CorfuRuntime[numRuntimes];

        for (int x = 0; x < rts.length; x++) {
            rts[x] = new CorfuRuntime(endpoint).connect();
        }
        log.info("Connected {} runtimes...", numRuntimes);

        service = Executors.newFixedThreadPool(numThreads);
        requestsCompleted = new LongAdder();
        recorder = new Recorder(TimeUnit.SECONDS.toMillis(120000), 5);
        traces = new SimpleTrace[numThreads];
        MetricsUtils.metricsReportingSetup(CorfuRuntime.getDefaultMetrics());
    }

    public void setArgs(String[] args) {
        Args cmdArgs = new Args();
        JCommander jc = JCommander.newBuilder()
                .addObject(cmdArgs)
                .build();
        jc.parse(args);

        if (cmdArgs.help) {
            jc.usage();
            System.exit(0);
        }
        numRuntimes = cmdArgs.numClients;
        numThreads = cmdArgs.numThreads;
        numRequests = cmdArgs.numRequests;
        endpoint = cmdArgs.endpoint;
    }
}

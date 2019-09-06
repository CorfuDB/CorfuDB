package org.corfudb.benchmarks;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;


/**
 * This class is the super class for all performance tests.
 * It set test parameters like how many clients to run, how many threads each client run,
 * and how many request one thread send.
 */
@Slf4j
public class BenchmarkTest {
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
    protected CorfuRuntime[] rts = null;

    public BenchmarkTest(String[] args) {
        setArgs(args);
        rts = new CorfuRuntime[numRuntimes];

        for (int x = 0; x < rts.length; x++) {
            rts[x] = new CorfuRuntime(endpoint).connect();
        }
        log.info("Connected {} runtimes...", numRuntimes);
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

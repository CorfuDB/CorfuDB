package org.corfudb.benchmark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.Getter;
import lombok.NonNull;

public class ParseArgs {
    /**
     * Number of runtimes
     */
    @Getter
    protected int numRuntimes;

    /**
     *
     */
    @Getter
    protected int numThreads;

    /**
     * Number of requests per thread.
     */
    @Getter
    protected int numRequests;

    @Getter
    protected int numStreams;
    /**
     * Server endpoint.
     */
    @Getter
    @NonNull
    protected String endpoint;

    /**
     * ratio of CorfuTable put operation.
     */
    @Getter
    protected double ratio;

    /**
     * operation name.
     */
    @Getter
    @NonNull
    protected String op;

    @Getter
    protected int keySize;

    ParseArgs(String[] args) {
        Args cmdArgs = new Args();
        JCommander jc = JCommander.newBuilder()
                .addObject(cmdArgs)
                .build();
        jc.parse(args);

        if (cmdArgs.help) {
            jc.usage();
            System.exit(0);
        }
        numRequests = cmdArgs.numRequests;
        numRuntimes = cmdArgs.numClients;
        numThreads = cmdArgs.numThreads;
        numStreams = cmdArgs.numStreams;
        endpoint = cmdArgs.endpoint;
        ratio = cmdArgs.ratio;
        op = cmdArgs.op;
        keySize = cmdArgs.keySize;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("ParseArgs: numRuntimes=").append(numRuntimes)
                .append(", numThreads=").append(numThreads)
                .append(", numStreams=").append(numStreams)
                .append(", numRequests=").append(numRequests)
                .append(", endpoint=").append(endpoint)
                .append(", ratio=").append(ratio)
                .append(", operation=").append(op)
                .append(", keysize=").append(keySize);
        return stringBuilder.toString();
    }

    public static class Args {
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

        @Parameter(names = {"--num-streams"}, description = "Number of streams", required = true)
        int numStreams;

        @Parameter(names = {"--ratio"}, description = "num of put operations / num of requests", required = false)
        double ratio;

        @Parameter(names = {"--op"}, description = "operation you want to test", required = true)
        String op;

        @Parameter(names = {"--key-size"}, description = "maximum number of keys you want to store", required = false)
        int keySize;
    }
}
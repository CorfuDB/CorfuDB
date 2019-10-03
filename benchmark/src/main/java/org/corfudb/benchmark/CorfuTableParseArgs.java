package org.corfudb.benchmark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class CorfuTableParseArgs extends ParseArgs{
    /**
     * Number of requests per thread.
     */
    protected int numRequests;

    /**
     * Number of streams.
     */
    protected int numStreams;

    /**
     * ratio of CorfuTable put operation.
     */
    protected double ratio;

    /**
     * operation name.
     */
    @NonNull
    protected String op;

    /**
     * Number of key.
     */
    protected int keyNum;

    /**
     * Size of value.
     */
    protected int valueSize;

    CorfuTableParseArgs(String[] args) {
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
        keyNum = cmdArgs.keyNum;
        valueSize = cmdArgs.valueSize;
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
                .append(", keyNum=").append(keyNum)
                .append(", valueSize=").append(valueSize);
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

        @Parameter(names = {"--ratio"}, description = "num of put operations / num of requests", required = true)
        double ratio;

        @Parameter(names = {"--op"}, description = "operation you want to test", required = true)
        String op;

        @Parameter(names = {"--key-num"}, description = "maximum number of keys you want to store", required = true)
        int keyNum;

        @Parameter(names = {"--value-size"}, description = "maximum number of keys you want to store", required = true)
        int valueSize;
    }
}
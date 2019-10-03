package org.corfudb.benchmark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class SequencerParseArgs extends ParseArgs{
    /**
     * Number of requests per thread.
     */
    protected int numRequests;

    /**
     * operation name.
     */
    @NonNull
    protected String op;

    SequencerParseArgs(String[] args) {
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
        endpoint = cmdArgs.endpoint;
        op = cmdArgs.op;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("ParseArgs: numRuntimes=").append(numRuntimes)
                .append(", numThreads=").append(numThreads)
                .append(", numRequests=").append(numRequests)
                .append(", endpoint=").append(endpoint)
                .append(", operation=").append(op);
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

        @Parameter(names = {"--op"}, description = "operation you want to test", required = true)
        String op;
    }
}
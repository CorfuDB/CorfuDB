package org.corfudb.benchmark;

import lombok.Getter;
import lombok.NonNull;

@Getter
public class ParseArgs {
    /**
     * Number of runtimes.
     */
    protected int numRuntimes;

    /**
     * Number of threads.
     */
    protected int numThreads;

    /**
     * Server endpoint.
     */
    @Getter
    @NonNull
    protected String endpoint;
}

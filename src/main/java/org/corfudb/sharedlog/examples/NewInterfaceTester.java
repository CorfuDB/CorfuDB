package org.corfudb.sharedlog.examples;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.view.Sequencer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;

import java.util.ArrayList;

public class NewInterfaceTester {

    private static final Logger log = LoggerFactory.getLogger(NewInterfaceTester.class);

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        String masteraddress = null;

        if (args.length >= 1) {
            masteraddress = args[0]; // TODO check arg.length
        } else {
            // throw new Exception("must provide master http address"); // TODO
            masteraddress = "http://localhost:8002/corfu";
        }

        long numTokens = 1000000;
        CorfuDBClient client = new CorfuDBClient(masteraddress);
        Sequencer s = new Sequencer(client);
        client.startViewManager();

        for (int numThreads = 1; numThreads <= 8; numThreads++)
        {
            long numTokensPerThread = numTokens / numThreads;

            log.info("Starting new interface test, threads=" + numThreads +", totaltokens="+ numTokens + ", tokensperthread=" + numTokensPerThread);

            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            log.info("Waiting for view to be ready...");
            client.waitForViewReady();
            log.info("View ready, starting test.");

            Callable<Void> r = () -> {
                for (long i = 0; i < numTokensPerThread; i++)
                {
                    s.getNext();
                }
                return null;
            };

            ArrayList<Callable<Void>> list = new ArrayList<Callable<Void>>();
            for (long i = 0; i < numThreads; i++)
            {
                list.add(r);
            }
            long startTime = System.currentTimeMillis();
            try {
                executor.invokeAll(list);
            } catch ( Exception e ) {}
            long endTime = System.currentTimeMillis();
            long actionsPerSec = Math.round((float)numTokens / ((endTime-startTime)/1000));
            long testTime = endTime-startTime;
            log.info("Total acquisitions/sec=" + actionsPerSec + ", test time=" + testTime);
        }
    }
}


package org.corfudb.tests;

import org.corfudb.client.CorfuDBClient;

import org.corfudb.client.view.Sequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;

import org.corfudb.client.abstractions.SharedLog;

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

        long numTokens = 100000;
        CorfuDBClient client = new CorfuDBClient(masteraddress);
        Sequencer s = new Sequencer(client);
        WriteOnceAddressSpace woas = new WriteOnceAddressSpace(client);
        SharedLog sl = new SharedLog(s, woas);
        client.startViewManager();

        for (int numThreads = 1; numThreads <= 8; numThreads++)
        {
            long numTokensPerThread = numTokens / numThreads;

            log.info("Starting new interface test, threads=" + numThreads +", totaltokens="+ numTokens + ", tokensperthread=" + numTokensPerThread);

            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            log.info("Waiting for view to be ready...");
            client.waitForViewReady();
            log.info("View ready, starting test.");

            byte[] testData = new byte[4096];
            for (int i = 0; i < 4096; i++)
            {
                testData[i] = (byte) i;
            }

            Callable<Void> r = () -> {
                for (long i = 0; i < numTokensPerThread; i++)
                {
                    sl.append(testData);
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
            long testTime = endTime-startTime;
            float actionsPerSec = (float)numTokens /((float)testTime/1000);
            log.info("Total acquisitions/sec=" + actionsPerSec + ", test time=" + testTime);
        }
    }
}


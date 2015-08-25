package org.corfudb.samples;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.collections.CDBSimpleMap;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.util.CorfuDBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Created by dalia on 8/20/15.
 */
public class HelloMap implements Runnable {
    private Logger log = LoggerFactory.getLogger(HelloMap.class);
    private static final String usage = "HelloMap <master> [nthreads]\n\n";

    public static void main(String[] args) throws Exception {

        System.out.println("creating test thread...");
        Thread r = new Thread(new HelloMap(args));
        r.start();
        synchronized (r) {
            r.join();
        }
    }

    private String masterString = null;
    private int nthreads = 2;
    CorfuDBRuntime cdr;
    ICorfuDBInstance instance;

    HelloMap(String[] args) {
        if (args.length < 1) {
            log.info("Usage: {}", usage);
            System.exit(1);
        }
        masterString = args[0];
        Map<String, Object> opts = new HashMap<String, Object>() {{
            put("--master", masterString);
        }};
        if (args.length > 1) {
            log.info("using {} threads",  args[1]);
            nthreads = Integer.parseInt(args[1]);
        }

        CorfuDBFactory cdbFactory = new CorfuDBFactory(opts);
        log.info("Creating CorfuDBRuntime...");
        cdr = cdbFactory.getRuntime();
        instance = cdr.getLocalInstance();

    }

    CDBSimpleMap<Integer, Integer> testMap = null;

    public void run() {

        Thread[] t = new Thread[nthreads];

       /*
         * The configuration master provides a resetAll command which resets the state
         * of the system. You should not use it in production, but it is very useful for
         * testing purposes.
         */
        System.out.println("resetting configuration...");
        instance.getConfigurationMaster().resetAll();

        UUID streamID = UUID.randomUUID();
        testMap = instance.openObject(streamID, CDBSimpleMap.class);
        int worksize = 8000 / nthreads;

        for (int j = 0; j < nthreads; j++) {
            log.info("starting thread {}", j);
            t[j] = new Thread(() -> {
                int r = new Random().nextInt();
                log.info("Thread starting puts with random seed r={}..", r);
                for (int i = 0; i < worksize; i++)
                    testMap.put(i, r+i);
                log.info("Thread starting gets..");
                for (int i = 0; i < worksize; i++) {
                    if (testMap.get(i) != r+i)
                        log.info("get({}) returns {}; another thread overwrote {}+{}={} (that's good)",
                                i, testMap.get(i), r, i, r+i
                        );

                }
                log.info("Thread done.");
            }); t[j].start();
        }

        log.info("waiting for all threads to finish ..");
        for (Thread tt : t) {
            try {
                tt.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        log.info("!!!!!!!!!!!!!!!All work completed!!!!!!!!!!!!!!!");
    }
}

package org.corfudb.samples;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.collections.CDBSimpleMap;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.util.CorfuDBFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by dalia on 8/20/15.
 */
public class HelloMap implements Runnable {
    private static final String usage = "HelloMap <master>\n\n";

    public static void main(String[] args) throws Exception {

        System.out.println("creating test thread...");
        Thread r = new Thread(new HelloMap(args));
        r.start();
        synchronized (r) {
            r.wait();
        }
    }

    private String masterString = null;
    CorfuDBRuntime cdr;
    ICorfuDBInstance instance;

    HelloMap(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: " + usage);
            System.exit(1);
        }
        masterString = args[0];
        Map<String, Object> opts = new HashMap<String, Object>() {{
            put("--master", masterString);
        }};

        CorfuDBFactory cdbFactory = new CorfuDBFactory(opts);
        System.out.println("Creating CorfuDBRuntime...");
        cdr = cdbFactory.getRuntime();
        instance = cdr.getLocalInstance();

    }

    public void run() {

       /*
         * The configuration master provides a resetAll command which resets the state
         * of the system. You should not use it in production, but it is very useful for
         * testing purposes.
         */
        System.out.println("resetting configuration...");
        instance.getConfigurationMaster().resetAll();

        UUID streamID = UUID.randomUUID();
        IStream s = instance.openStream(streamID);
        CDBSimpleMap<Integer, Integer> testMap = instance.openObject(streamID, CDBSimpleMap.class);

        testMap.put(0, 0);
        System.out.println("get(0): " + testMap.get(0) );
        System.out.println("done");
    }
}

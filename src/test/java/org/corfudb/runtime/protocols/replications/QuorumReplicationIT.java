package org.corfudb.runtime.protocols.replications;

import org.corfudb.infrastructure.SimpleLogUnitServer;
import org.corfudb.infrastructure.StreamingSequencerServer;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.view.CorfuDBView;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.util.CorfuInfrastructureBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dalia on 8/15/15.
 */
public class QuorumReplicationIT implements Runnable {
    private static final String usage =
            "QuorumReplicationIT\n\n";

    CorfuDBView view = null;
    String masterString;
    CorfuDBRuntime cdr;

    private static Map<String, Object> luConfigMap = new HashMap<String,Object>() {
        {
            put("capacity", 200000);
            put("ramdisk", true);
            put("pagesize", 4096);
            put("trim", 0);
        }
    };
    private static CorfuInfrastructureBuilder infrastructure =
            CorfuInfrastructureBuilder.getBuilder()
                    .addSequencer(9001, StreamingSequencerServer.class, "cdbsts", null)
                    .addLoggingUnit(9010, 0, SimpleLogUnitServer.class, "cdbslu", luConfigMap)
                    .addLoggingUnit(9011, 0, SimpleLogUnitServer.class, "cdbslu", luConfigMap)
                    .addLoggingUnit(9012, 0, SimpleLogUnitServer.class, "cdbslu", luConfigMap)
        //            .setReplicationProtocol("cdbqr")
                    .start(9002);


    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        System.out.println("creating test thread...");
        Thread r = new Thread(new QuorumReplicationIT(args));
        r.start();
        synchronized(r) {r.wait(); }
    }

    QuorumReplicationIT(String[] args) {
    }

    public void run() {

        /* To interact with a CorfuDB instance, we first need a runtime to interact with.
         * We can get an instance of CorfuDBRuntime by using the factory.
         */
        System.out.println("Creating CorfuDBRuntime...");
        cdr = CorfuDBRuntime.getRuntime(infrastructure.getConfigString());

        /* The basic unit of CorfuDB is called an instance. It encapsulates the logging units,
         */
        ICorfuDBInstance instance = cdr.getLocalInstance();


       /*
         * The configuration master provides a resetAll command which resets the state
         * of the system. You should not use it in production, but it is very useful for
         * testing purposes.
         */
        /* System.out.println("resetting configuration...");
        instance.getConfigurationMaster().resetAll();*/


        /* check health of Configuration Master by trying to retrieve view
         */

        long timeout = 10000;
        System.out.println("Trying simple connection with Corfu components: config-master, sequencer, and logging units (will timeout in " + timeout/1000 + " secs)...");

        Thread b = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("trying to connect to config-master...");
                view = cdr.getView();

                System.out.println("trying to ping all view components...: " + view.isViewAccessible() );
                synchronized (this) { notify();}
            } } );
        b.start();

        synchronized (b) {
            try {
                b.wait(timeout);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (view == null) {
            System.out.println("cannot retrieve configuration from ConfigMaster. Try checking with browser whether the master URL is correct: " + masterString);
            System.exit(1);
        }

        System.out.println("HelloCorfu test finished successfully");
        synchronized (this) { notify(); }

    }
}

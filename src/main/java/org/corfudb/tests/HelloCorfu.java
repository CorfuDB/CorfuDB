package org.corfudb.tests;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.view.CorfuDBView;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.util.CorfuDBFactory;

import java.awt.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by dalia on 8/15/15.
 */
public class HelloCorfu implements Runnable {
    private static final String usage =
            "HelloCorfu <master>\n\n";

    CorfuDBView view = null;
    long pos = -1;
    String masterString;
    CorfuDBRuntime cdr;

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        System.out.println("creating test thread...");
        Thread r = new Thread(new HelloCorfu(args));
        r.start();
        synchronized(r) {r.wait(); }
    }

    HelloCorfu(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: " + usage);
            System.exit(1);
        }
        masterString = args[0];
    }

    public void run() {
        /*  This example uses docopt to parse command line arguments.
         *  For more information on how to use docopt, see http://docopt.org
         */
        Map<String, Object> opts = new HashMap<String, Object>() {{
            put("--master", masterString);
        }};

        /* The convenience class CorfuDBFactory allows us to create
         * CorfuDB class instances based on command line configuration parsed by docopt.
         */
        System.out.println("Creating CorfuDBFactory...");
        CorfuDBFactory cdbFactory = new CorfuDBFactory(opts);

        /* To interact with a CorfuDB instance, we first need a runtime to interact with.
         * We can get an instance of CorfuDBRuntime by using the factory.
         */
        System.out.println("Creating CorfuDBRuntime...");
        cdr = cdbFactory.getRuntime();

        /* The basic unit of CorfuDB is called an instance. It encapsulates the logging units,
         * configuration master and sequencer. It provides the primary method of interacting with
          * CorfuDB.
          *
          * To get an instance, call the .getLocalInstance() method on CorfuDBRuntime.
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

        System.out.println("Trying simple connection with Corfu components: config-master, sequencer, and logging units (will timeout in 5 secs)...");

        Thread b = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("trying to connect to config-master...");
                view = cdr.getView();
                synchronized (this) { notify();}
            } } );
        b.start();

        synchronized (b) {
            try {
                b.wait(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (view == null) {
            System.out.println("cannot retrieve configuration from ConfigMaster. Try checking with browser whether the master URL is correct: " + masterString);
            System.exit(1);
        }

        b = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("trying to connect to sequencer...");
                pos = instance.getSequencer().getCurrent();
                System.out.println("pos=" + pos);
                synchronized (this) { notify();}
            } } );
        b.start();

        synchronized (b) {
            try {
                b.wait(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (pos < 0) {
            System.out.println("cannot connect with sequencer");
            System.exit(1);
        }



        System.out.println("HelloCorfu test finished successfully");
        synchronized (this) { notify(); }

    }
}

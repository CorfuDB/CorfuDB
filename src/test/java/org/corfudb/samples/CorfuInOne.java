package org.corfudb.samples;

import org.corfudb.infrastructure.NettyLogUnitServer;
import org.corfudb.infrastructure.NettyStreamingSequencerServer;
import org.corfudb.infrastructure.SimpleLogUnitServer;
import org.corfudb.infrastructure.StreamingSequencerServer;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.collections.CDBCounter;
import org.corfudb.runtime.protocols.logunits.CorfuDBSimpleLogUnitProtocol;
import org.corfudb.runtime.protocols.logunits.NettyLogUnitProtocol;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.view.CorfuDBView;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.util.CorfuDBFactory;
import org.corfudb.util.CorfuInfrastructureBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by dalia on 9/11/15.
 */
public class CorfuInOne {

    public static void main(String args[]) {

        Map<String, Object> luConfigMap = new HashMap<String,Object>() {
            {
                put("capacity", 200000);
                put("ramdisk", true);
                put("pagesize", 4096);
                put("trim", 0);
            }
        };
        CorfuInfrastructureBuilder cbuilder = new CorfuInfrastructureBuilder().
                addLoggingUnit(7002, 0, NettyLogUnitServer.class, "nlu", luConfigMap).
                addLoggingUnit(7003, 0, NettyLogUnitServer.class, "nlu", luConfigMap).
                addLoggingUnit(7004, 0, NettyLogUnitServer.class, "nlu", luConfigMap).
                addSequencer(7001, NettyStreamingSequencerServer.class, "nsss", null).
  //              setReplication("cdbqr").
                start(9999) ;

        CorfuDBRuntime cdr = CorfuDBRuntime.getRuntime("http://localhost:9999/corfu");

        ICorfuDBInstance cinstance = cdr.getLocalInstance();

       /* check health of Configuration Master by trying to retrieve view
         */

        long timeout = 10000;
        System.out.println("Trying simple connection with Corfu components: config-master, sequencer, and logging units (will timeout in " + timeout/1000 + " secs)...");

        Thread b = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("trying to connect to config-master...");
                CorfuDBView view = cdr.getView();

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

        Thread work = new Thread(new Runnable() {
            @Override
            public void run() {
                IStream iStream = cdr.getLocalInstance().openStream(UUID.randomUUID());
                try {
                    for (int j = 0; j < 10000; j++)
                        iStream.append("hello");
                } catch (IOException e) {
                    e.printStackTrace();
                }
//                CDBCounter ctr = cdr.getLocalInstance().openObject(UUID.randomUUID(), CDBCounter.class);
//                System.out.println("ctr: " + ctr.increment() );
                synchronized (this) {notify(); }
            }}
            ); work.start();

        synchronized (work) {
            try {
                work.wait(timeout);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("HelloCorfu test finished successfully");

    }
}

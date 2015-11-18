package org.corfudb.samples;

import org.corfudb.infrastructure.NettyLogUnitServer;
import org.corfudb.infrastructure.NettyStreamingSequencerServer;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.view.CorfuDBView;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.runtime.view.LocalCorfuDBInstance;
import org.corfudb.util.CorfuITBuilder;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by dalia on 9/11/15.
 */
public class CorfuInOne {

    public static void main(String args[]) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {

        Map<String, Object> luConfigMap = new HashMap<String,Object>() {
            {
                put("capacity", 200000);
                put("ramdisk", true);
                put("pagesize", 4096);
                put("trim", 0);
            }
        };
        CorfuDBView view = new CorfuITBuilder().
                addLoggingUnit(7002, 0, NettyLogUnitServer.class, "nlu", luConfigMap).
                addLoggingUnit(7003, 0, NettyLogUnitServer.class, "nlu", luConfigMap).
                addLoggingUnit(7004, 0, NettyLogUnitServer.class, "nlu", luConfigMap).
                addSequencer(7001, NettyStreamingSequencerServer.class, "nsss", null).
  //              setReplication("cdbqr").
                getView(9999).
                start() ;

        System.out.println("view:" + view.getSerializedJSONView());
        ICorfuDBInstance cinstance = new LocalCorfuDBInstance(0);

       /* check health of Configuration Master by trying to retrieve view
         */

        long timeout = 10000;
        System.out.println("Trying simple connection with Corfu components: config-master, sequencer, and logging units (will timeout in " + timeout/1000 + " secs)...");

        Thread b = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("trying to ping all view components...: " + cinstance.getViewJanitor().isViewAccessible() );
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
                IStream iStream = cinstance.openStream(UUID.randomUUID());
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

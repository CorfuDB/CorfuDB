package org.corfudb.tests;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.view.Sequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.abstractions.SharedLog;

import org.corfudb.client.OutOfSpaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import com.esotericsoftware.kryonet.Server;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Connection;
import java.io.IOException;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.view.Sequencer;
import org.corfudb.client.abstractions.Stream;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.abstractions.SharedLog;
import org.corfudb.client.entries.CorfuDBStreamEntry;
import org.corfudb.client.entries.BundleEntry;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import com.codahale.metrics.*;
import java.util.concurrent.TimeUnit;
import java.util.Map.Entry;
import org.docopt.Docopt;
import java.util.Map;

import org.corfudb.client.abstractions.Bundle;
import java.util.UUID;

public class StreamObjectClient {

    private static final Logger log = LoggerFactory.getLogger(StreamObjectClient.class);
    static final MetricRegistry m = new MetricRegistry();

    public static  String getTimerString(Timer t)
    {
        Snapshot s = t.getSnapshot();
        return String.format("total/opssec %d/%2.2f min/max/avg/p95/p99 %2.2f/%2.2f/%2.2f/%2.2f/%2.2f",
                                t.getCount(),
                                convertRate(t.getMeanRate(), TimeUnit.SECONDS),
                                convertDuration(s.getMin(), TimeUnit.MILLISECONDS),
                                convertDuration(s.getMax(), TimeUnit.MILLISECONDS),
                                convertDuration(s.getMean(), TimeUnit.MILLISECONDS),
                                convertDuration(s.get95thPercentile(), TimeUnit.MILLISECONDS),
                                convertDuration(s.get99thPercentile(), TimeUnit.MILLISECONDS)
                            );
    }

    protected static double convertDuration(double duration, TimeUnit unit) {
        double durationFactor = 1.0 / unit.toNanos(1);
        return duration * durationFactor;
    }

    protected static double convertRate(double rate, TimeUnit unit) {
        double rateFactor = unit.toSeconds(1);
        return rate * rateFactor;
    }

    protected static void printResults(MetricRegistry m)
    {
        System.out.format("Total time: %2.2f ms\n", convertDuration(m.getTimers().get("total").getSnapshot().getMin(), TimeUnit.MILLISECONDS));
        for (Entry<String,Timer> e : m.getTimers().entrySet())
        {
                Timer t = e.getValue();
                System.out.format("%-48s : %s\n", e.getKey(), getTimerString(t));
        }
    }


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

            final int numthreads = 1;
            final int txns = 10;

            CorfuDBClient client = new CorfuDBClient(masteraddress);
            client.startViewManager();
            final int clientNum = 1;
            Stream s = new Stream(client, new UUID(1,0), 10, 100, false);

            ArrayList<UUID> remotes = new ArrayList<UUID>();
            remotes.add(new UUID(0,0));
            remotes.add(new UUID(0,1));

            Timer t_total = m.timer("total");
            Timer.Context c_total = t_total.time();

            Timer t_bundleapply = m.timer("bundle apply");
            Timer.Context c_bundleapply = t_bundleapply.time();
            Bundle b = new Bundle(s, remotes, null, true);
            b.apply();
            c_bundleapply.stop();

            /* now we can do some work.
             * We already did one txn.
             * */
            for (int txn = 1; txn < txns; txn++)
            {
                Timer t_localtx = m.timer("localtx");
                Timer.Context c_localtx = t_localtx.time();
                s.append(new StreamObjectServer.StreamClientObjectWrapper(clientNum));
                c_localtx.stop();
            }

            /* hmm, ok now we have to make sure the TX was successful.*/
            for (int i = 0; i < (txns + remotes.size()); i++)
            {
                Timer t_localrx = m.timer("localrx");
                Timer.Context c_localrx = t_localrx.time();
                CorfuDBStreamEntry cdse = s.readNextEntry();
                if (cdse.payload instanceof StreamObjectServer.StreamObjectWrapper)
                {
                    /* remote ack */
                    StreamObjectServer.StreamObjectWrapper sow = (StreamObjectServer.StreamObjectWrapper) cdse.payload;
                    log.debug("remote ack from {}", sow.serverNum);
                }
                c_localrx.stop();
            }

           c_total.stop();
           printResults(m);

           System.exit(1);

    }
}


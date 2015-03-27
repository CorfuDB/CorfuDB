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
import com.esotericsoftware.kryonet.Client;

import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Connection;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import com.codahale.metrics.*;
import java.util.concurrent.TimeUnit;
import java.util.Map.Entry;
import org.docopt.Docopt;
import java.util.Map;

import java.io.IOException;
public class ObjectClient {

    private static final Logger log = LoggerFactory.getLogger(ObjectClient.class);
    static final MetricRegistry m = new MetricRegistry();

    private static ConcurrentHashMap<Integer, ObjectServer.TwoPLRequest> rpcMap = new ConcurrentHashMap<Integer, ObjectServer.TwoPLRequest>();
    private static ConcurrentHashMap<Integer, ObjectServer.TwoPLResponse> rpcResponseMap = new ConcurrentHashMap<Integer, ObjectServer.TwoPLResponse>();
    private static ConcurrentHashMap<Integer, Timer.Context> timerMap = new ConcurrentHashMap<Integer, Timer.Context>();
    private static AtomicInteger counter = new AtomicInteger();

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

    private static ObjectServer.TwoPLRequest generateRequest(ArrayList<Integer> list, Timer.Context tc, boolean read, boolean write, boolean unlock, int key, String value)
    {
        ObjectServer.TwoPLRequest tpr = new ObjectServer.TwoPLRequest();
        tpr.id = counter.getAndIncrement();
        tpr.readLock = read;
        tpr.writeLock = write;
        tpr.unlock = unlock;
        tpr.key = key;
        tpr.value = value;
        rpcMap.put(tpr.id, tpr);
        timerMap.put(tpr.id, tc);
        list.add(tpr.id);
        return tpr;
    }

    private static void waitForRPCs(ArrayList<Integer> ids)
    {
        for (Integer id : ids)
        {
            ObjectServer.TwoPLRequest tpr_orig = rpcMap.get(id);
            try {
                while (rpcResponseMap.get(id) == null)
                {
                    Thread.sleep(100);
                }
            }
            catch(InterruptedException ie) {}
        }
    }

    //Dear java, please please please add multiline strings in java9
    private static final String doc =
         "ObjectClient, the benchmark tool.\n\n"
        +"Usage:\n"
        +"  corfudb_bench [--txns <numtxns>] [--runs <runs>] [--numObjs <numObjs>] [--test <testtype>]\n"
        +"  corfudb_bench (-h | --help)\n"
        +"  corfudb_bench --version\n\n"
        +"Options:\n"
        +"  -t <numtxns>, --txns <numxns>           number of transactions to burst [default: 1]\n"
        +"  -r <runs>, --runs <runs>                number of runs [default: 1]\n"
        +"  -n <numObjs>, --numObjs <numObjs>       number of objects at each remote [default: 1]\n"
        +"  --test <testtype>                       type of test to run [default: 2PL]\n"
        +"  --h --help                              show this screen\n"
        +"  --version                               show version.\n";

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        Map<String,Object> opts = new Docopt(doc).withVersion("git").parse(args);
        final int numObjs = Integer.parseInt((String)opts.get("--numObjs"));
        final int runs = Integer.parseInt((String)opts.get("--runs"));
        final int txns = Integer.parseInt((String)opts.get("--txns"));
        final String testType = (String) opts.get("--test");
        log.info("numobjs={}, runs={}, txns={} test={}", numObjs, runs, txns, testType);
        ArrayList<String> hostList = new ArrayList<String>();
        hostList.add("104.40.57.207");
        hostList.add("191.237.23.229");
        hostList.add("104.40.221.153");
        hostList.add("23.102.29.11");
        ArrayList<Client> clientList = new ArrayList<Client>();
        for (String host : hostList)
        {
           Client c = new Client();
           c.getKryo().register(ObjectServer.TwoPLRequest.class);
           c.getKryo().register(ObjectServer.TwoPLResponse.class);
           new Thread(c).start();
           c.connect(5000, host, 5000, 5000);
           c.addListener( new Listener() {
                public void received(Connection connection, Object obj)
                {
                    if (obj instanceof ObjectServer.TwoPLResponse)
                    {
                        ObjectServer.TwoPLResponse tpr = (ObjectServer.TwoPLResponse) obj;
                        ObjectServer.TwoPLRequest tpr_orig = rpcMap.get(tpr.id);
                        synchronized(tpr_orig)
                        {
                            tpr_orig.notifyAll();
                        }
                        timerMap.get(tpr.id).stop();
                        rpcResponseMap.put(tpr.id, tpr);
                        //log.info("Response[{}]: {}", tpr.id, tpr.success);
                    }
                }}
            );
           clientList.add(c);
        }
        for (int run = 0; run < runs; run++)
        {
           Timer t_total = m.timer("total");
           Timer.Context c_total = t_total.time();

           //log.info("Phase 1: Acquiring locks");
           for (int txn = 0; txn < txns; txn++)
           {
               ArrayList<Integer> rpcList = new ArrayList<Integer>();
               for (Client c : clientList)
               {
                    for (int i = 0; i < numObjs; i++)
                    {
                       Timer t_lock = m.timer("Acquire lock");
                       Timer.Context c_lock = t_lock.time();
                       c.sendTCP(generateRequest(rpcList, c_lock, false, true, false, i, "hello world"));
                    }
               }
               waitForRPCs(rpcList);
               //log.info("Phase 2: Releasing locks");
               if (testType.equals("2PL"))
               {
                   rpcList.clear();
                   for (Client c: clientList)
                   {
                       for (int i = 0; i < numObjs; i++)
                       {
                           Timer t_rlock = m.timer("Release lock");
                           Timer.Context c_rlock = t_rlock.time();

                            c.sendTCP(generateRequest(rpcList,c_rlock, false, false, true, i, ""));
                       }
                   }
                   waitForRPCs(rpcList);
               }
           }
           c_total.stop();
        }
           printResults(m);
           System.exit(1);
    }
}


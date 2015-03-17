package org.corfudb.tests;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.CorfuDBViewSegment;
import org.corfudb.client.IServerProtocol;
import org.corfudb.client.view.Sequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.abstractions.SharedLog;
import org.corfudb.client.abstractions.Stream;
import org.corfudb.client.configmasters.IConfigMaster;
import org.corfudb.client.OutOfSpaceException;
import org.corfudb.client.OverwriteException;
import org.corfudb.client.TrimmedException;
import org.corfudb.client.UnwrittenException;
import org.corfudb.tests.benchtests.IBenchTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;


import java.util.Map;
import java.util.Map.Entry;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;

import java.util.UUID;

import org.docopt.Docopt;

import com.codahale.metrics.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import java.util.concurrent.TimeUnit;
import java.lang.Thread;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import java.lang.reflect.Constructor;

public class corfudb_bench {

    private static final Logger log = LoggerFactory.getLogger(corfudb_bench.class);
    static final MetricRegistry registry = new MetricRegistry();

    //Dear java, please please please add multiline strings in java9
    private static final String doc =
         "corfudb_bench, the corfudb benchmark tool.\n\n"
        +"Usage:\n"
        +"  corfudb_bench benchmark <master-address> [--threads <numthreads>] [--ops <operations>] [--streams <numstreams>] [--allocation-size <size>] [--window-size <size>] [--payload-size <size>]\n"
        +"  corfudb_bench list-tests\n"
        +"  corfudb_bench (-h | --help)\n"
        +"  corfudb_bench --version\n\n"
        +"Options:\n"
        +"  -t <numthreads>, --threads <numthreads>  number of threads to spawn for each test [default: 1]\n"
        +"  -o <operations>, --ops <operations>      number of operations to perform for each test [default: 10000]\n"
        +"  -s <numstreams>, --streams <numstreams>  number of streams to test (for multi-stream tests) [default: 4]\n"
        +"  -ws <size>, --window-size <size>         number of outstanding operations to allow, 1=synchronous [default: 1]\n"
        +"  -as <size>, --allocation-size <size>     number of entries to allocate for this stream\n"
        +"  -ps <size>, --payload-size <size>        size of payload for read/write operations in bytes [default: 4096]\n"
        +"  --h --help                              show this screen\n"
        +"  --version                               show version.\n";


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
        System.out.println(getTimerString(m.getTimers().get("action")));
        for (Entry<String,Timer> e : m.getTimers().entrySet())
        {
            if(!e.getKey().equals("total") && !e.getKey().equals("action"))
            {
                Timer t = e.getValue();
                System.out.format("%-48s : %s\n", e.getKey(), getTimerString(t));
            }
        }
    }
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        Map<String,Object> opts = new Docopt(doc).withVersion("git").parse(args);
        Reflections reflections = new Reflections("org.corfudb.tests.benchtests", new SubTypesScanner(false));
        Set<Class<? extends Object>> allClasses = reflections.getSubTypesOf(Object.class);
        ArrayList<IBenchTest> tests = new ArrayList<IBenchTest>();

        for(Class<? extends Object> c : allClasses)
        {
            try {
                if (c != IBenchTest.class)
                {
                    Object o = c.getConstructor().newInstance();
                    tests.add((IBenchTest) o);
                }
            }
            catch (Exception e)
            {
                log.debug("Error generating testobject", e);
            }
        }

        if ((Boolean)opts.get("list-tests"))
        {
            System.out.println("Available tests:");
            for (IBenchTest test : tests)
            {
                System.out.format("%s\n", test.getClass().toString());
            }
        }
        else
        {
            try (CorfuDBClient testClient = new CorfuDBClient((String)opts.get("<master-address>")))
            {
                testClient.startViewManager();
                testClient.waitForViewReady();
                IConfigMaster cm = (IConfigMaster) testClient.getView().getConfigMasters().get(0);
                cm.resetAll();
                for (IBenchTest test : tests)
                {
                    System.out.format("Starting %s (sync):\n", test.getClass().toString());
                    MetricRegistry m = test.runTest(opts);
                    printResults(m);
                    cm.resetAll();
                    System.out.format("Starting %s (async):\n", test.getClass().toString());
                    m = test.runTestAsync(opts);
                    printResults(m);
                    cm.resetAll();
                }
            }
        }
    }
}

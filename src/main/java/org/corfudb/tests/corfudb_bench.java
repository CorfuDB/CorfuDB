package org.corfudb.tests;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.CorfuDBViewSegment;
import org.corfudb.client.IServerProtocol;
import org.corfudb.client.view.Sequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.abstractions.SharedLog;

import org.corfudb.client.OutOfSpaceException;
import org.corfudb.client.OverwriteException;
import org.corfudb.client.TrimmedException;
import org.corfudb.client.UnwrittenException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;

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


public class corfudb_bench {

    private static final Logger log = LoggerFactory.getLogger(corfudb_bench.class);
    static final MetricRegistry registry = new MetricRegistry();

    //Dear java, please please please add multiline strings in java9
    private static final String doc =
         "corfudb_bench, the corfudb benchmark tool.\n\n"
        +"Usage:\n"
        +"  corfudb_bench benchmark <master-address> [--threads <numthreads>] [--ops <operations>] [--window-size <size>]\n"
        +"  corfudb_bench (-h | --help)\n"
        +"  corfudb_bench --version\n\n"
        +"Options:\n"
        +"  -t <numthreads>, --threads <numthreads>  number of threads to spawn for each test [default: 1]\n"
        +"  -o <operations>, --ops <operations>      number of operations to perform for each test [default: 10000]\n"
        +"  -ws <size>, --window-size <size>         number of outstanding operations to allow, 1=synchronous [default: 1]\n"
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
        BenchTest[] tests = new BenchTest[] {
            new PingTest(),
            new AppendTest(),
            new ReadTest()
        };

        for (BenchTest test : tests)
        {
            System.out.format("Starting %s (sync):\n", test.getClass().toString());
            MetricRegistry m = test.runTest(opts);
            printResults(m);
            System.out.format("Starting %s (async):\n", test.getClass().toString());
            m = test.runTestAsync(opts);
            printResults(m);
        }
    }

    interface BenchTest extends AutoCloseable {
        void doSetup(Map<String, Object> args);
        void doRun(Map<String,Object> args, MetricRegistry m);
        default CorfuDBClient getClient(Map<String,Object> args) {
            return new CorfuDBClient((String)args.get("<master-address>"));
        }
        default int getNumThreads(Map<String,Object> args) {
            return Integer.parseInt((String)args.get("--threads"));
        }
        default int getNumOperations(Map<String,Object> args) {
            return Integer.parseInt((String)args.get("--ops"));
        }
        default int getWindowSize(Map<String,Object> args) {
            return Integer.parseInt((String)args.get("--window-size"));
        }
        default int getNumActionsPerThread(Map<String,Object> args) {
            return getNumOperations(args) / getNumThreads(args);
        }
        default MetricRegistry runTestAsync(Map<String,Object> args)
        {
            final MetricRegistry m = new MetricRegistry();
            final AtomicInteger totalCompleted = new AtomicInteger();
            int totalDispatched = 0;
            try (BenchTest t = this)
            {
                t.doSetup(args);
                ExecutorService executor = Executors.newFixedThreadPool(getNumThreads(args));
                Timer t_action = m.timer("action");
                Timer t_total = m.timer("total");
                final Timer.Context c_total = t_total.time();
                do {
                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                            final Timer.Context c_action = t_action.time();
                            doRun(args, m);
                            totalCompleted.incrementAndGet();
                            c_action.stop();
                    }, executor);

                    totalDispatched++;
                    while (totalDispatched - totalCompleted.get() > getWindowSize(args) ||
                            (totalDispatched >= getNumOperations(args) && totalDispatched != totalCompleted.get()))
                    {
                    }
                } while (totalCompleted.get() < getNumOperations(args));
                c_total.stop();
                executor.shutdownNow();
            }
            catch (Exception e)
            {
                log.error("Error running test " + this.getClass().toString(), e);
            }
            return m;
        }
        default MetricRegistry runTest(Map<String, Object> args)
        {
            final MetricRegistry m = new MetricRegistry();
            try (BenchTest t = this)
            {
                t.doSetup(args);
                ExecutorService executor = Executors.newFixedThreadPool(getNumThreads(args));
                Timer t_action = m.timer("action");
                Timer t_total = m.timer("total");
                Callable<Void> r = () -> {
                    for (int i =0; i < getNumActionsPerThread(args); i++)
                    {
                        doRun(args, m);
                    }
                    return null;
                };
                ArrayList<Callable<Void>> list = new ArrayList<Callable<Void>>();
                for (long i = 0; i < getNumThreads(args); i++)
                {
                    list.add(r);
                }
                final Timer.Context c_total = t_total.time();
                try {
                executor.invokeAll(list);} catch(InterruptedException ie) {}
                c_total.stop();
                executor.shutdownNow();
            }
            catch (Exception e)
            {
                log.error("Error running test " + this.getClass().toString(), e);
            }

            return m;
        }
    }

    @SuppressWarnings({"rawtypes","unchecked"})
    static class PingTest implements BenchTest {
        PingTest() {}
        CorfuDBClient c;

        public void doSetup(Map<String, Object> args)
        {
            c = getClient(args);
            c.startViewManager();
            c.waitForViewReady();
        }

        public void close() {
            c.close();
        }

        public void doRun(Map<String,Object> args, MetricRegistry m)
        {
            for (IServerProtocol s : c.getView().getSequencers())
            {
                Timer t_sequencer = m.timer("ping sequencer: " + s.getFullString());
                Timer.Context c_sequencer = t_sequencer.time();
                s.ping();
                c_sequencer.stop();
            }
            for (CorfuDBViewSegment vs : c.getView().getSegments())
            {
                for (List<IServerProtocol> lsp : vs.getGroups())
                {
                    for (IServerProtocol lu : lsp)
                    {
                        Timer t_logunit = m.timer("ping logunit: " + lu.getFullString());
                        Timer.Context c_logunit = t_logunit.time();
                        lu.ping();
                        c_logunit.stop();
                    }
                }
            }
        }
    }

    @SuppressWarnings({"rawtypes","unchecked"})
    static class AppendTest implements BenchTest {
        CorfuDBClient c;
        Sequencer s;
        WriteOnceAddressSpace woas;
        byte[] data;

        AppendTest() {}

        public void doSetup(Map<String, Object> args)
        {
            c = getClient(args);
            c.startViewManager();
            data = new byte[4096];
            s = new Sequencer(c);
            woas = new WriteOnceAddressSpace(c);
            c.waitForViewReady();
        }

        public void close() {
            c.close();
        }

        public void doRun(Map<String,Object> args, MetricRegistry m)
        {
            Timer t_sequencer = m.timer("Acquire token");
            Timer.Context c_sequencer = t_sequencer.time();
            long token = s.getNext();
            c_sequencer.stop();
            Timer t_logunit = m.timer("Append data");
            Timer.Context c_logunit = t_logunit.time();
            try {
            woas.write(token, data);}
            catch (OverwriteException oe) {}
            catch (TrimmedException te) {}
            c_logunit.stop();
        }
    }

    @SuppressWarnings({"rawtypes","unchecked"})
    static class ReadTest implements BenchTest {
        CorfuDBClient c;
        Sequencer s;
        WriteOnceAddressSpace woas;
        AtomicLong l;
        ReadTest() {}

        public void doSetup(Map<String, Object> args)
        {
            c = getClient(args);
            c.startViewManager();
            s = new Sequencer(c);
            woas = new WriteOnceAddressSpace(c);
            l = new AtomicLong();
            c.waitForViewReady();
        }

        public void close() {
            c.close();
        }

        public void doRun(Map<String,Object> args, MetricRegistry m)
        {
            Timer t_logunit = m.timer("Read data");
            Timer.Context c_logunit = t_logunit.time();
            try {
            woas.read(l.getAndIncrement());}
            catch (UnwrittenException oe) {}
            catch (TrimmedException te) {}
            c_logunit.stop();
        }
    }

}

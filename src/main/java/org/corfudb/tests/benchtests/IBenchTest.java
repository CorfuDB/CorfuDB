package org.corfudb.tests.benchtests;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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


public interface IBenchTest extends AutoCloseable {
    static final Logger log = LoggerFactory.getLogger(IBenchTest.class);
    void doSetup(Map<String, Object> args);
    void doRun(Map<String,Object> args, long runNum, MetricRegistry m);
    void close();
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
    default int getPayloadSize(Map<String,Object> args) {
        return Integer.parseInt((String)args.get("--payload-size"));
    }
    default int getNumStreams(Map<String,Object> args) {
        return Integer.parseInt((String)args.get("--streams"));
    }
    default int getNumActionsPerThread(Map<String,Object> args) {
        return getNumOperations(args) / getNumThreads(args);
    }
    default int getStreamAllocationSize(Map<String, Object> args) {
        if (args.get("--allocation-size") == null)
        {
            return Math.max(2,getNumOperations(args)/getNumStreams(args));
        }
        return Integer.parseInt((String) args.get("--allocation-size"));
    }
    default int getSingleStreamAllocationSize(Map<String, Object> args) {
        if (args.get("--allocation-size") == null)
        {
            return Math.max(2,getNumOperations(args));
        }
        return Integer.parseInt((String) args.get("--allocation-size"));
    }
    default MetricRegistry runTestAsync(Map<String,Object> args)
    {
        final MetricRegistry m = new MetricRegistry();
        final AtomicLong totalCompleted = new AtomicLong();
        final AtomicLong numRequested = new AtomicLong();
        int totalDispatched = 0;
        try (IBenchTest t = this)
        {
            t.doSetup(args);
            ExecutorService executor = Executors.newFixedThreadPool(getNumThreads(args));
            Timer t_action = m.timer("action");
            Timer t_total = m.timer("total");
            final Timer.Context c_total = t_total.time();
            do {
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        final Timer.Context c_action = t_action.time();
                        doRun(args, numRequested.getAndIncrement(), m);
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
            close();
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
        AtomicLong al = new AtomicLong(0);
        try (IBenchTest t = this)
        {
            t.doSetup(args);
            ExecutorService executor = Executors.newFixedThreadPool(getNumThreads(args));
            Timer t_action = m.timer("action");
            Timer t_total = m.timer("total");
            Callable<Void> r = () -> {
                for (long i =0; i < getNumActionsPerThread(args); i++)
                {
                    final Timer.Context c_action = t_action.time();
                    doRun(args, al.getAndIncrement(),m);
                    c_action.stop();
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
            close();
        }
        catch (Exception e)
        {
            log.error("Error running test " + this.getClass().toString(), e);
        }

        return m;
    }
}



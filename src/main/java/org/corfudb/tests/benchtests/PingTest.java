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


@SuppressWarnings({"rawtypes","unchecked"})
public class PingTest implements IBenchTest {
    public PingTest() {}
    CorfuDBClient c;

    @Override
    public void doSetup(Map<String, Object> args)
    {
        c = getClient(args);
        c.startViewManager();
        c.waitForViewReady();
    }

    @Override
    public void close() {
        c.close();
    }

    @Override
    public void doRun(Map<String,Object> args, long runNum, MetricRegistry m)
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


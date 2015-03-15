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
public class AppendTest implements IBenchTest {
    CorfuDBClient c;
    Sequencer s;
    WriteOnceAddressSpace woas;
    byte[] data;

    public AppendTest() {}

    @Override
    public void doSetup(Map<String, Object> args)
    {
        c = getClient(args);
        c.startViewManager();
        data = new byte[getPayloadSize(args)];
        s = new Sequencer(c);
        woas = new WriteOnceAddressSpace(c);
        c.waitForViewReady();
    }

    @Override
    public void close() {
        c.close();
    }

    @Override
    public void doRun(Map<String,Object> args, long runNum, MetricRegistry m)
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


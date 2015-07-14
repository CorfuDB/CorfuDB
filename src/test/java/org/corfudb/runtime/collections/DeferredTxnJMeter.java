package org.corfudb.runtime.collections;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.smr.DeferredTransaction;
import org.corfudb.runtime.smr.ISMREngine;
import org.corfudb.runtime.smr.ITransactionCommand;
import org.corfudb.runtime.smr.SimpleSMREngine;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleTimestamp;
import org.corfudb.runtime.view.ICorfuDBInstance;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Measure what the read bottleneck (throughput) is for deferred transactions without optimizations
 *
 * Created by amytai on 7/9/15.
 */
public class DeferredTxnJMeter extends AbstractJavaSamplerClient {
    CorfuDBRuntime runtime;
    ICorfuDBInstance instance;
    CDBSimpleMap<Integer, Integer> map;
    UUID uuid;

    static Lock l = new ReentrantLock();
    static Boolean reset = false;

    final int NUMTXNS = 1;

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
        // Read each of the txns, one at a time; each thread must process/execute every entry.
        CDBSimpleMap<Integer, Integer> localMap = instance.openObject(uuid, CDBSimpleMap.class);
        SampleResult result = new SampleResult();
        result.setSuccessful(true);
        result.sampleStart();
        localMap.getSMREngine().sync(null);
        result.sampleEnd();
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setupTest(JavaSamplerContext context) {
        runtime = CorfuDBRuntime.getRuntime("http://localhost:12700/corfu");
        instance = runtime.getLocalInstance();

        l.lock();
        if (!reset)
        {
            instance.getConfigurationMaster().resetAll();
            reset = true;
        }
        l.unlock();

        uuid = UUID.randomUUID();

        map = instance.openObject(uuid, CDBSimpleMap.class);

        // Insert deferred txns, just to get a throughput estimate.
        Random r = new Random();
        for (int i = 0; i < NUMTXNS; i++) {
            int key = r.nextInt(1000);
            DeferredTransaction tx = new DeferredTransaction(instance);
            final CDBSimpleMap<Integer, Integer> txMap = map;
            tx.setTransaction((ITransactionCommand) (opts) -> {
                txMap.put(key, key);
                return true;
            });
            try {
                ITimestamp ts = tx.propose();
            } catch (Exception e) {
                System.exit(1);
            }
        }

        super.setupTest(context);
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        runtime.close();
        super.teardownTest(context);
    }
}

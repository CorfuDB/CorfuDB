package org.corfudb.runtime.collections;

import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.corfudb.infrastructure.NettyLogUnitServer;
import org.corfudb.infrastructure.NettyStreamingSequencerServer;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.smr.DeferredTransaction;
import org.corfudb.runtime.smr.ITransactionCommand;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.util.CorfuInfrastructureBuilder;

import java.util.HashMap;
import java.util.Map;
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

    static CorfuInfrastructureBuilder infrastructure;
    static Lock l = new ReentrantLock();
    static Boolean reset = false;

    final int NUMTXNS = 1000;

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
        // Read each of the txns, one at a time; each thread must process/execute every entry.
        SampleResult result = new SampleResult();
        result.setSuccessful(true);
        result.sampleStart();
        Random r = new Random();
        int key = r.nextInt(1000);
        DeferredTransaction tx = new DeferredTransaction(instance);
        final CDBSimpleMap<Integer, Integer> txMap = map;
        tx.setTransaction((ITransactionCommand) (opts) -> {
            if (txMap.get(key) == null) {
                txMap.put(key, key);
                return false;
            }
            return true;
        });
        try {
            ITimestamp ts = tx.propose();
        } catch (Exception e) {
            System.exit(1);
        }
        map.getSMREngine().sync(null);
        result.sampleEnd();
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setupTest(JavaSamplerContext context) {

        Map<String, Object> luConfigMap = new HashMap<String,Object>() {
            {
                put("capacity", 200000);
                put("ramdisk", true);
                put("pagesize", 4096);
                put("trim", 0);
            }
        };

        l.lock();
        if (!reset)
        {
            infrastructure = CorfuInfrastructureBuilder.getBuilder()
                    .addSequencer(9201, NettyStreamingSequencerServer.class, "nsss", null)
                    .addLoggingUnit(9200, 0, NettyLogUnitServer.class, "nlu", luConfigMap)
                    .start(9002);

            reset = true;
            uuid = UUID.randomUUID();
        }
        l.unlock();

        runtime = CorfuDBRuntime.getRuntime(infrastructure.getConfigString());
        instance = runtime.getLocalInstance();
        uuid = UUID.randomUUID();

        map = instance.openObject(uuid, CDBSimpleMap.class);
/*
        // Insert deferred txns, just to get a throughput estimate.
        Random r = new Random();
        for (int i = 0; i < Integer.parseInt(context.getParameter("LoopController.loops")); i++) {
            int key = r.nextInt(1000);
            DeferredTransaction tx = new DeferredTransaction(instance);
            final CDBSimpleMap<Integer, Integer> txMap = map;
            tx.setTransaction((ITransactionCommand) (opts) -> {
                if (txMap.get(key) == null) {
                    txMap.put(key, key);
                    return false;
                }
                return true;
            });
            try {
                ITimestamp ts = tx.propose();
            } catch (Exception e) {
                System.exit(1);
            }
        }
*/
        super.setupTest(context);
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        runtime.close();
        l.lock();
        if (reset) {
            infrastructure.shutdownAndWait();
            reset = false;
        }
        l.unlock();
        super.teardownTest(context);
    }
}

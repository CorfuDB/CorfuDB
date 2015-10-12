package org.corfudb.runtime.collections;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.corfudb.infrastructure.NettyLogUnitServer;
import org.corfudb.infrastructure.NettyStreamingSequencerServer;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.smr.SimpleSMREngine;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.util.CorfuInfrastructureBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by mwei on 6/11/15.
 */
public class CDBSimpleMapJMeter extends AbstractJavaSamplerClient {
    CorfuDBRuntime runtime;
    ICorfuDBInstance instance;
    CDBSimpleMap<String, String> map;

    static CorfuInfrastructureBuilder infrastructure;
    static UUID uuid;
    static Lock l = new ReentrantLock();
    static Boolean reset = false;

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
        SampleResult result = new SampleResult();
        result.setSampleLabel("Put+Get");
        result.setSuccessful(true);
        SampleResult putResult = new SampleResult();
        putResult.setSampleLabel("Put random 1KB entry");
        SampleResult getResult = new SampleResult();
        getResult.setSampleLabel("Get random 1KB entry");
        Random r = new Random();
        String rPut = Integer.toString(r.nextInt(7));
        String rGet = Integer.toString(r.nextInt(7));
        result.sampleStart();
        try {
            putResult.sampleStart();
            map.put(rPut, RandomStringUtils.random(1024));
            putResult.sampleEnd();
            putResult.setSuccessful(true);
            getResult.sampleStart();
            map.get(rGet);
            getResult.sampleEnd();
            getResult.setSuccessful(true);
        }
        catch (Exception e)
        {
            result.setSuccessful(false);
        }
        result.sampleEnd();
        result.addSubResult(putResult);
        result.addSubResult(getResult);
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

        map = instance.openObject(uuid, new ICorfuDBInstance.OpenObjectArgs<CDBSimpleMap>(
                CDBSimpleMap.class,
                SimpleSMREngine.class,
                true
        ));

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

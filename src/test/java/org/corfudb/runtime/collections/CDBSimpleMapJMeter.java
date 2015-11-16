package org.corfudb.runtime.collections;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.corfudb.infrastructure.NettyLogUnitServer;
import org.corfudb.infrastructure.NettyStreamingSequencerServer;
import org.corfudb.runtime.CorfuDBRuntimeIT;
import org.corfudb.runtime.smr.SimpleSMREngine;
import org.corfudb.runtime.view.CorfuDBView;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.runtime.view.LocalCorfuDBInstance;

import java.lang.reflect.InvocationTargetException;
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
    CDBSimpleMap<String, String> map;

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

        LocalCorfuDBInstance instance = CorfuDBRuntimeIT.generateInstance();

        UUID uuid = UUID.randomUUID();
        map = instance.openObject(uuid, new ICorfuDBInstance.OpenObjectArgs<CDBSimpleMap>(
                CDBSimpleMap.class,
                SimpleSMREngine.class,
                true
        ));

        super.setupTest(context);
    }
}

package org.corfudb.infrastructure;

import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.runtime.view.ISequencer;
import org.corfudb.util.CorfuInfrastructureBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by mwei on 6/11/15.
 */
public class SimpleSequencerServerJMeter extends AbstractJavaSamplerClient {

    CorfuDBRuntime runtime;
    ICorfuDBInstance instance;
    ISequencer sequencer;

    static CorfuInfrastructureBuilder infrastructure;
    static Lock l = new ReentrantLock();
    static Boolean reset = false;

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
        SampleResult result = new SampleResult();
        result.setSampleLabel("Token Acquisition");
        result.sampleStart();
        sequencer.getNext();
        result.sampleEnd();
        result.setSuccessful(true);
        return result;
    }

    @Override
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
            infrastructure =
                    CorfuInfrastructureBuilder.getBuilder()
                            .addSequencer(9001, SimpleSequencerServer.class, "cdbss", null)
                            .start(9002);
            reset = true;
        }
        l.unlock();

        runtime = CorfuDBRuntime.getRuntime(infrastructure.getConfigString());
        instance = runtime.getLocalInstance();
        sequencer = instance.getSequencer();

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

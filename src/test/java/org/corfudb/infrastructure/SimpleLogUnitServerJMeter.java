package org.corfudb.infrastructure;

import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.runtime.view.IWriteOnceAddressSpace;
import org.corfudb.util.CorfuInfrastructureBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by mwei on 6/11/15.
 */
public class SimpleLogUnitServerJMeter extends AbstractJavaSamplerClient {

    CorfuDBRuntime runtime;
    ICorfuDBInstance instance;
    IWriteOnceAddressSpace w;

    static CorfuInfrastructureBuilder infrastructure;
    static Lock l = new ReentrantLock();
    static Boolean reset = false;
    static AtomicLong al = new AtomicLong();

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
        SampleResult result = new SampleResult();
        result.setSampleLabel("1KB write");
        result.setSuccessful(true);
        result.sampleStart();
        try {
            w.write(al.getAndIncrement(), new byte[1024]);
        }
        catch (Exception e)
        {
            result.setSuccessful(false);
        }
        result.sampleEnd();
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
            infrastructure = CorfuInfrastructureBuilder.getBuilder()
                    .addLoggingUnit(9000, 0, SimpleLogUnitServer.class, "cdbslu", luConfigMap)
                    .start(9002);

            reset = true;
        }
        l.unlock();

        runtime = CorfuDBRuntime.getRuntime(infrastructure.getConfigString());
        instance = runtime.getLocalInstance();
        w = instance.getAddressSpace();
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

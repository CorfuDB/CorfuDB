package org.corfudb.infrastructure;

import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.runtime.view.IWriteOnceAddressSpace;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by amytai on 7/22/15.
 */
public class RocksLogUnitServerJMeter extends AbstractJavaSamplerClient {

    CorfuDBRuntime runtime;
    ICorfuDBInstance instance;
    IWriteOnceAddressSpace w;

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
        runtime = CorfuDBRuntime.getRuntime("http://localhost:12701/corfu");
        instance = runtime.getLocalInstance();
        w = instance.getAddressSpace();

        l.lock();
        if (!reset)
        {
            instance.getConfigurationMaster().resetAll();
            reset = true;
        }
        l.unlock();
        super.setupTest(context);
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        runtime.close();
        super.teardownTest(context);
    }
}

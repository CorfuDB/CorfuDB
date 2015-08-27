package org.corfudb.infrastructure;

import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.protocols.logunits.CorfuNewLogUnitProtocol;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.runtime.view.IWriteOnceAddressSpace;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by mwei on 6/11/15.
 */
public class NewLogUnitServerJMeter extends AbstractJavaSamplerClient {

    CorfuNewLogUnitProtocol p;

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
            p.write(al.getAndIncrement(), Collections.emptySet(), ByteBuffer.wrap(new byte[1024]));
        }
        catch (Exception e)
        {
            result.setSuccessful(false);
        }
        result.sampleEnd();
        return result;
    }

    @Override
    public void setupTest(JavaSamplerContext context)
    {

        p = new CorfuNewLogUnitProtocol("localhost", 12908, new HashMap<String,String>(), 0L);

        l.lock();
        if (!reset)
        {
            try {
                p.reset(0);
            } catch (Exception e) {}
            reset = true;
        }
        l.unlock();
        super.setupTest(context);
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        super.teardownTest(context);
    }
}

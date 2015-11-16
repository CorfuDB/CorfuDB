package org.corfudb.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.corfudb.runtime.CorfuDBRuntimeIT;
import org.corfudb.runtime.protocols.logunits.INewWriteOnceLogUnit;
import org.corfudb.runtime.protocols.logunits.NettyLogUnitProtocol;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.runtime.view.IStreamingSequencer;
import org.corfudb.util.RandomOpenPort;

import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by mwei on 6/11/15.
 */
@Slf4j
public class NettyLogUnitServerJMeter extends AbstractJavaSamplerClient {

    ICorfuDBInstance instance;
    NettyLogUnitProtocol proto;

    UUID streamID;

    static Lock l = new ReentrantLock();
    static Boolean reset = false;
    static int port;

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
        SampleResult result = new SampleResult();
        result.setSampleLabel("write");
        result.sampleStart();
        UUID streamID = UUID.randomUUID();
        String test = "Hello World";
        proto.write(0, Collections.singleton(streamID), 0, test);
        result.sampleEnd();
        result.setSuccessful(true);
        return result;
    }

    @Override
    public void setupTest(JavaSamplerContext context) {
        instance = CorfuDBRuntimeIT.generateInstance();
        proto =
                new NettyLogUnitProtocol("localhost", port, Collections.emptyMap(), 0);
        streamID = UUID.randomUUID();
        super.setupTest(context);
    }
}

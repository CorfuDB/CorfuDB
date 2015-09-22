package org.corfudb.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.protocols.logunits.INewWriteOnceLogUnit;
import org.corfudb.runtime.protocols.logunits.NettyLogUnitProtocol;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.runtime.view.IStreamingSequencer;
import org.corfudb.util.CorfuInfrastructureBuilder;
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

    CorfuDBRuntime runtime;
    ICorfuDBInstance instance;
    NettyLogUnitProtocol proto;

    UUID streamID;

    static CorfuInfrastructureBuilder infrastructure;
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
        l.lock();
        if (!reset)
        {
            port = RandomOpenPort.getOpenPort();
            infrastructure =
                    CorfuInfrastructureBuilder.getBuilder()
                            .addLoggingUnit(port, 0, NettyLogUnitServer.class, "nlu", new HashMap<String, Object>())
                            .start(7775);
            try {
                Thread.sleep(500);
            } catch (Exception e)
            {

            }
            reset = true;
        }
        l.unlock();

        runtime = CorfuDBRuntime.getRuntime(infrastructure.getConfigString());
        instance = runtime.getLocalInstance();
        proto =
                new NettyLogUnitProtocol("localhost", port, Collections.emptyMap(), 0);
        streamID = UUID.randomUUID();
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

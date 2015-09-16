package org.corfudb.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.runtime.view.IStreamingSequencer;
import org.corfudb.util.CorfuInfrastructureBuilder;

import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by mwei on 6/11/15.
 */
@Slf4j
public class NettyStreamingSequencerServerJMeter extends AbstractJavaSamplerClient {

    CorfuDBRuntime runtime;
    ICorfuDBInstance instance;
    IStreamingSequencer sequencer;

    UUID streamID;

    static CorfuInfrastructureBuilder infrastructure;
    static Lock l = new ReentrantLock();
    static Boolean reset = false;

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
        SampleResult result = new SampleResult();
        result.setSampleLabel("Token Acquisition");
        result.sampleStart();
        sequencer.getNext(streamID);
        result.sampleEnd();
        result.setSuccessful(true);
        return result;
    }

    @Override
    public void setupTest(JavaSamplerContext context) {
        l.lock();
        if (!reset)
        {
            infrastructure =
                    CorfuInfrastructureBuilder.getBuilder()
                            .addSequencer(7779, NettyStreamingSequencerServer.class, "nsss", null)
                            .start(7777);
            try {
                Thread.sleep(5000);
            } catch (Exception e)
            {

            }
            reset = true;
        }
        l.unlock();

        runtime = CorfuDBRuntime.getRuntime(infrastructure.getConfigString());
        instance = runtime.getLocalInstance();
        sequencer = instance.getStreamingSequencer();

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

package org.corfudb.infrastructure;

import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.runtime.view.ISequencer;

/**
 * Created by mwei on 6/11/15.
 */
public class SimpleSequencerServerJMeter extends AbstractJavaSamplerClient {

    ICorfuDBInstance instance;
    ISequencer sequencer;

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
        instance =
                CorfuDBRuntime.getRuntime("http://localhost:12700/corfu").getLocalInstance();
        sequencer = instance.getSequencer();
        super.setupTest(context);
    }
}

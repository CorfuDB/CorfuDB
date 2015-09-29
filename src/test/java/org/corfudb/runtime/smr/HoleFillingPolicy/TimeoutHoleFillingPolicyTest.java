package org.corfudb.runtime.smr.HoleFillingPolicy;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.HoleEncounteredException;
import org.corfudb.runtime.collections.CDBSimpleMap;
import org.corfudb.runtime.protocols.configmasters.MemoryConfigMasterProtocol;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.SimpleStream;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.runtime.view.Serializer;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.github.marschall.junitlambda.LambdaAssert.assertRaises;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 8/14/15.
 */
public class TimeoutHoleFillingPolicyTest {

    ICorfuDBInstance instance;
    IStream s;

    @Before
    public void generateStream()
    {
        MemoryConfigMasterProtocol.inMemoryClear();
        CorfuDBRuntime cdr = CorfuDBRuntime.createRuntime("memory");
        instance = cdr.getLocalInstance();
        s = instance.openStream(UUID.randomUUID());
    }

    public void generateHole()
            throws Exception
    {
        s.reserve(1);
    }

  //  @Test
    public void TimeoutPolicyFillsHoles()
            throws Exception
    {
        generateHole();
        TimeoutHoleFillPolicy policy = new TimeoutHoleFillPolicy(5, ChronoUnit.MILLIS);
        assertRaises(s::readNextEntry, HoleEncounteredException.class);
        try {
            s.readNextEntry();
        } catch(HoleEncounteredException hee)
        {
            policy.apply(hee, s);
            //hole should still be filled at this point.
            assertRaises(s::readNextEntry, HoleEncounteredException.class);
            Thread.sleep(100);
            policy.apply(hee, s);
            //now hole should be filled
            s.readNextEntry();
        }
    }
}

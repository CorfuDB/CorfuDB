package org.corfudb.infrastructure;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.OverwriteException;
import org.corfudb.runtime.TrimmedException;
import org.corfudb.runtime.protocols.logunits.CorfuDBSimpleLogUnitProtocol;
import org.corfudb.runtime.protocols.logunits.IWriteOnceLogUnit;
import org.corfudb.runtime.view.ConfigurationMaster;
import org.corfudb.runtime.view.IConfigurationMaster;
import org.corfudb.util.CorfuInfrastructureBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.github.marschall.junitlambda.LambdaAssert.assertRaises;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 5/20/15.
 */
public class SimpleLogUnitServerIT  {

    CorfuDBRuntime cdr;
    CorfuDBSimpleLogUnitProtocol lu;

    static Map<String, Object> luConfigMap = new HashMap<String,Object>() {
        {
            put("capacity", 200000);
            put("ramdisk", true);
            put("pagesize", 4096);
            put("trim", 0);
        }
    };

    CorfuInfrastructureBuilder infrastructure =
            CorfuInfrastructureBuilder.getBuilder()
                    .addSequencer(9001, StreamingSequencerServer.class, "cdbsts", null)
                    .addLoggingUnit(9000, 0, SimpleLogUnitServer.class, "cdbslu", luConfigMap)
                    .start(9002);

    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            System.out.println("Starting test: " + description.getMethodName());
        }
    };


    @Before
    public void getCDR() {
        cdr = CorfuDBRuntime.createRuntime(infrastructure.getConfigString());
        cdr.waitForViewReady();
        cdr.getLocalInstance().getConfigurationMaster().resetAll();
        lu = ((CorfuDBSimpleLogUnitProtocol) cdr.getView().getSegments().get(0).getGroups().get(0).get(0));
    }

    @After
    public void shutdown() {
        infrastructure.shutdownAndWait();
    }

    private static byte[] getTestPayload(int size)
    {
        byte[] test = new byte[size];
        for (int i = 0; i < size; i++)
        {
            test[i] = (byte)(i % 255);
        }
        return test;
    }

    @Test
    public void canReadWrite1KB() throws Exception {
        byte[] t = getTestPayload(1024);
        lu.write(0, Collections.singleton("fake stream"), t);
        byte[] o = lu.read(0, "fake stream");

        assertThat(t)
                .isEqualTo(o);
    }

    @Test
    public void canReadWrite10KB() throws Exception {
        byte[] t = getTestPayload(10240);
        lu.write(0, Collections.singleton("fake stream"), t);
        byte[] o = lu.read(0, "fake stream");

        assertThat(t)
                .isEqualTo(o);
    }

    @Test
    public void canReadWrite100KB() throws Exception {
        byte[] t = getTestPayload(102400);
        lu.write(0, Collections.singleton("fake stream"), t);
        byte[] o = lu.read(0, "fake stream");

        assertThat(t)
                .isEqualTo(o);
    }


    @Test
    public void canReadWrite1MB() throws Exception {
        byte[] t = getTestPayload(1048576);
        lu.write(0, Collections.singleton("fake stream"), t);
        byte[] o = lu.read(0, "fake stream");

        assertThat(t)
                .isEqualTo(o);
    }

    @Test
    public void overwriteCausesException() throws Exception {
        byte[] t = getTestPayload(1024);
        lu.write(0, Collections.singleton("fake stream"), t);
        assertRaises(() -> lu.write(0, Collections.singleton("fake stream"), t), OverwriteException.class);
    }

    @Test
    public void trimmableSpace() throws Exception {
        byte[] t = getTestPayload(1024);
        lu.write(0, Collections.singleton("fake stream"), t);
        lu.trim(0);

        //trimmed address either cause a trimmed exception
        //or are equal to the old value.
        try {
            byte[] o = lu.read(0, "fake stream");
            assertThat(o)
                    .isEqualTo(t);
        }
        catch (TrimmedException te)
        {

        }
        //trimmed addresses must cause an overwrite exception
        assertRaises(() -> lu.write(0, Collections.singleton("fake stream"), t), OverwriteException.class);
    }

    @Test
    public void highestAddressIsHighest() throws Exception {
        byte[] t = getTestPayload(1024);

        assertThat(lu.highestAddress())
                .isEqualTo(-1);

        lu.write(0, Collections.singleton("fake stream"), t);

        assertThat(lu.highestAddress())
                .isEqualTo(0);

        lu.write(100, Collections.singleton("fake stream"), t);

        assertThat(lu.highestAddress())
                .isEqualTo(100);
    }
}

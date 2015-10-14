package org.corfudb.runtime.view;

import org.corfudb.infrastructure.NettyLogUnitServer;
import org.corfudb.infrastructure.NettyStreamingSequencerServer;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.util.CorfuInfrastructureBuilder;
import org.corfudb.util.RandomOpenPort;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.UUID;

/**
 * Created by mwei on 8/26/15.
 */
public class StreamAddressSpaceIT {

    CorfuInfrastructureBuilder infrastructure;
    CorfuDBRuntime runtime;
    ICorfuDBInstance instance;

    @Before
    public void setup()
    {
        infrastructure =
                CorfuInfrastructureBuilder.getBuilder()
                .addSequencer(RandomOpenPort.getOpenPort(), NettyStreamingSequencerServer.class, "nsss", null)
                .addLoggingUnit(RandomOpenPort.getOpenPort(), 0, NettyLogUnitServer.class, "nlu", null)
                .start(RandomOpenPort.getOpenPort());

        runtime = CorfuDBRuntime.getRuntime(infrastructure.getConfigString());
        instance = runtime.getLocalInstance();
    }

   @Test
    public void addressSpaceWriteRead()
            throws Exception
    {
        IStreamAddressSpace s = instance.getStreamAddressSpace();
        String test = "hello world";
        UUID id = UUID.randomUUID();
        assertThat(instance.getView().getSegments().get(0).getGroups().get(0).get(0).ping())
                .isTrue();
        s.write(0, Collections.singleton(id), test);
        IStreamAddressSpace.StreamAddressSpaceEntry entry = s.read(0);
        assertThat(entry.getGlobalIndex())
                .isEqualTo(0);
        assertThat(entry.getPayload())
                .isEqualTo(test);
        assertThat(entry.getStreams().contains(id))
                .isEqualTo(true);
    }

    /** Test whether or not we can reset the caches */
    @Test
    public void resetCacheTest()
    {
        /** Get the stream address space to test. */
        IStreamAddressSpace s = instance.getStreamAddressSpace();
        String test = "hello world";
        /** Write to a random stream. */
        UUID id = UUID.randomUUID();
        s.write(0, Collections.singleton(id), test);
        /** Read, so the entry gets cached. */
        IStreamAddressSpace.StreamAddressSpaceEntry entry = s.read(0);
        /** The entry should not be null at this point. */
        assertThat(entry)
                .isNotNull();

        /** Reset the instance, so that nothing is on the log unit anymore. */
        instance.getConfigurationMaster().resetAll();
        /** Manually reset the cache. The cache should now be empty. */
        s.resetCaches();

        /** Read from the address space, which should now miss in the cache. */
        entry = s.read(0);
        /** The entry should return NULL, which means that the cache was reset.*/
        assertThat(entry)
                .isNull();
    }

    @After
    public void tearDown()
    {
        infrastructure
                .shutdownAndWait();
    }
}

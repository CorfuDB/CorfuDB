package org.corfudb.runtime;

import org.corfudb.runtime.protocols.configmasters.MemoryConfigMasterProtocol;
import org.corfudb.runtime.protocols.logunits.MemoryLogUnitProtocol;
import org.corfudb.runtime.protocols.sequencers.MemorySequencerProtocol;
import org.corfudb.runtime.view.*;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.assertj.core.api.Assertions.*;

import java.util.*;

/**
 * Created by mwei on 4/30/15.
 */
public class CorfuDBRuntimeTest {
    private static UUID uuid = UUID.randomUUID();

    @Test
    public void MemoryCorfuDBRuntimeHasComponents() {
        MemoryConfigMasterProtocol.inMemoryClear();
        CorfuDBRuntime runtime = CorfuDBRuntime.createRuntime("memory");
        CorfuDBView view = runtime.getView();
        assertNotNull(view);
        assertThat(view.getConfigMasters().get(0))
                .isInstanceOf(MemoryConfigMasterProtocol.class);
        assertThat(view.getSequencers().get(0))
                .isInstanceOf(MemorySequencerProtocol.class);
        assertThat(view.getSegments().get(0).getGroups().get(0).get(0))
                .isInstanceOf(MemoryLogUnitProtocol.class);
    }

    @Test
    public void MemoryCorfuDBViewChangeTest() {
        MemoryConfigMasterProtocol.inMemoryClear();
        CorfuDBRuntime runtime = CorfuDBRuntime.createRuntime("memory");
        CorfuDBView view = runtime.getView();
        assertNotNull(view);
        LayoutMonitor cm = new LayoutMonitor(runtime);
        cm.resetAll();
        view = runtime.getView();
        assertNotNull(view);
    }

    /*
    @Test
    @SuppressWarnings("unchecked")
    public void AllStreamsCanBeCreatedByRuntime(){
        MemoryConfigMasterProtocol.inMemoryClear();
        CorfuDBRuntime runtime = CorfuDBRuntime.createRuntime("memory");
        HashSet<Class<? extends IStream>> streams = new HashSet<Class<? extends IStream>>();
        Reflections reflections = new Reflections("org.corfudb.runtime.stream", new SubTypesScanner(false));
        Set<Class<? extends Object>> allClasses = reflections.getSubTypesOf(Object.class);

        for(Class<? extends Object> c : allClasses)
        {
            try {
                if (Arrays.asList(c.getInterfaces()).contains(IStream.class) && !c.isInterface())
                {
                    streams.add((Class<? extends IStream>) c);
                }
            }
            catch (Exception e)
            {
            }
        }

        streams.stream().forEach(p -> {
            try {
                IStream stream = runtime.openStream(UUID.nameUUIDFromBytes(new byte[]{0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0}), p);
                assertThat(stream)
                        .isInstanceOf(IStream.class);
            } catch (Exception e) {
                e.printStackTrace(System.out);
                Assert.fail("Exception while creating stream of type " + p.getName() + ": " + e.getMessage());
            }
        });
    }
    */

}

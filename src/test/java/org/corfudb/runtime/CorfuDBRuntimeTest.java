package org.corfudb.runtime;

import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.configmasters.IConfigMaster;
import org.corfudb.runtime.protocols.configmasters.MemoryConfigMasterProtocol;
import org.corfudb.runtime.protocols.logunits.MemoryLogUnitProtocol;
import org.corfudb.runtime.protocols.sequencers.MemorySequencerProtocol;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.view.*;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.assertj.core.api.Assertions.*;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

import java.util.*;

/**
 * Created by mwei on 4/30/15.
 */
public class CorfuDBRuntimeTest {
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
        ConfigurationMaster cm = new ConfigurationMaster(runtime);
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
    @Test
    public void MemoryCorfuDBFailoverTest()
    throws Exception
    {
        //this is an in-memory request.
        HashMap<String, Object> MemoryView = new HashMap<String, Object>();
        MemoryView.put("epoch", 0L);

        MemoryView.put("logid", UUID.randomUUID().toString());
        MemoryView.put("pagesize", 4096);

        LinkedList<String> configMasters = new LinkedList<String>();
        configMasters.push("mcm://localhost:0");
        MemoryView.put("configmasters", configMasters);

        LinkedList<String> sequencers = new LinkedList<String>();
        sequencers.push("ms://localhost:0");
        MemoryView.put("sequencers", sequencers);

        HashMap<String,Object> layout = new HashMap<String,Object>();
        LinkedList<HashMap<String,Object>> segments = new LinkedList<HashMap<String,Object>>();
        HashMap<String,Object> segment = new HashMap<String,Object>();
        segment.put("start", 0L);
        segment.put("sealed", 0L);
        LinkedList<HashMap<String,Object>> groups = new LinkedList<HashMap<String,Object>>();
        HashMap<String,Object> group0 = new HashMap<String,Object>();
        LinkedList<String> group0nodes = new LinkedList<String>();
        group0nodes.add("mlu://localhost:0");
        group0nodes.add("mlu://localhost:1");
        group0.put("nodes", group0nodes);
        groups.add(group0);
        segment.put("groups", groups);
        segments.add(segment);
        layout.put("segments", segments);
        MemoryView.put("layout", layout);

        /* get a test view */
        MemoryConfigMasterProtocol.inMemoryClear();
        CorfuDBView view = new CorfuDBView(MemoryView);
        MemoryConfigMasterProtocol.memoryConfigMasters.get(0).setInitialView(view);
        CorfuDBRuntime runtime = CorfuDBRuntime.createRuntime("custom");

        /* Write and make sure it is written to both in memory log units */
        Sequencer s = new Sequencer(runtime);
        WriteOnceAddressSpace woas = new WriteOnceAddressSpace(runtime);
        woas.write(s.getNext(), "Hello World".getBytes());

        assertThat(MemoryLogUnitProtocol.memoryUnits.get(0).read(0))
                .isEqualTo("Hello World".getBytes());
        assertThat(MemoryLogUnitProtocol.memoryUnits.get(1).read(0))
                .isEqualTo("Hello World".getBytes());

        /* cause a unit to fail */
        MemoryLogUnitProtocol.memoryUnits.get(1).simulateFailure(true);
        woas.write(s.getNext(), "Hello World 2".getBytes());

        /* make sure that it is written to unit 0 */
        assertThat(MemoryLogUnitProtocol.memoryUnits.get(0).read(1))
                .isEqualTo("Hello World 2".getBytes());

        /* make sure that reads work */
        assertThat(woas.read(1))
                .isEqualTo("Hello World 2".getBytes());

        /* make sure that transient failures aren't an issue */
        MemoryLogUnitProtocol.memoryUnits.get(1).simulateFailure(false);
        woas.write(s.getNext(), "Hello World 3".getBytes());

        assertThat(woas.read(1))
                .isEqualTo("Hello World 2".getBytes());
        assertThat(woas.read(2))
                .isEqualTo("Hello World 3".getBytes());

        /* and make sure that writes to the wrong epoch fail */
    }

}

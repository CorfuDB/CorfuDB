package org.corfudb.runtime;

import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.logunits.CorfuDBSimpleLogUnitProtocol;
import org.corfudb.runtime.protocols.logunits.IWriteOnceLogUnit;
import org.corfudb.runtime.protocols.sequencers.IStreamSequencer;
import org.corfudb.runtime.view.*;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.UUID;

import static com.github.marschall.junitlambda.LambdaAssert.assertRaises;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 5/18/15.
 */
public class CorfuDBRuntimeIT {
    @Test
    public void isCorfuViewAccessible()
    {
        CorfuDBRuntime cdr = new CorfuDBRuntime("http://localhost:12700/corfu");
        cdr.waitForViewReady();
        assertThat(cdr.getView())
                .isNotNull();
    }


    @Test
    public void isCorfuViewUsable() throws Exception
    {
        CorfuDBRuntime cdr = new CorfuDBRuntime("http://localhost:12700/corfu");

        cdr.waitForViewReady();
        assertThat(cdr.getView())
                .isNotNull();

        IConfigurationMaster cm = new ConfigurationMaster(cdr);
        cm.resetAll();

        Sequencer s = new Sequencer(cdr);
        assertThat(s.getCurrent())
                .isEqualTo(0);

        IWriteOnceAddressSpace woas = new WriteOnceAddressSpace(cdr);
        long addr = s.getNext();
        woas.write(addr, "hello world".getBytes());
        assertThat(woas.read(addr))
                .isEqualTo("hello world".getBytes());
    }

    @Test
    public void isCorfuResettable() throws Exception
    {
        CorfuDBRuntime cdr = new CorfuDBRuntime("http://localhost:12700/corfu");
        cdr.waitForViewReady();
        IConfigurationMaster cm = new ConfigurationMaster(cdr);
        cm.resetAll();

        Sequencer s = new Sequencer(cdr);
        assertThat(s.getCurrent())
                .isEqualTo(0);
        s.getNext();
        cm.resetAll();
        assertThat(s.getCurrent())
                .isEqualTo(0);

        IWriteOnceAddressSpace woas = new WriteOnceAddressSpace(cdr);
        long addr = s.getNext();
        woas.write(addr, "hello world".getBytes());
        assertThat(woas.read(addr))
                .isEqualTo("hello world".getBytes());

        cm.resetAll();
        addr = s.getNext();
        woas.write(addr, "hello world 2".getBytes());
        assertThat(woas.read(addr))
                .isEqualTo("hello world 2".getBytes());
    }

    @Test
    public void CorfuLogunitFailoverTest() throws Exception {
        CorfuDBRuntime cdr = new CorfuDBRuntime("http://localhost:12700/corfu");
        cdr.waitForViewReady();
        IConfigurationMaster cm = new ConfigurationMaster(cdr);
        cm.resetAll();

        CorfuDBView oldView = cdr.getView();

        /* Forcibly install a dual logunit configuration. */
        HashMap<String, Object> MemoryView = new HashMap<String, Object>();
        Long epoch = cdr.getView().getEpoch() + 1;
        MemoryView.put("epoch", epoch);

        MemoryView.put("logid", cdr.getView().getUUID().toString());
        MemoryView.put("pagesize", 4096);

        LinkedList<String> configMasters = new LinkedList<String>();
        configMasters.push("cdbcm://localhost:12700");
        MemoryView.put("configmasters", configMasters);

        LinkedList<String> sequencers = new LinkedList<String>();
        sequencers.push("cdbss://localhost:12600");
        MemoryView.put("sequencers", sequencers);

        HashMap<String, Object> layout = new HashMap<String, Object>();
        LinkedList<HashMap<String, Object>> segments = new LinkedList<HashMap<String, Object>>();
        HashMap<String, Object> segment = new HashMap<String, Object>();
        segment.put("start", 0L);
        segment.put("sealed", 0L);
        LinkedList<HashMap<String, Object>> groups = new LinkedList<HashMap<String, Object>>();
        HashMap<String, Object> group0 = new HashMap<String, Object>();
        LinkedList<String> group0nodes = new LinkedList<String>();
        group0nodes.add("cdbslu://localhost:12800");
        group0nodes.add("cdbslu://localhost:12801");
        group0.put("nodes", group0nodes);
        groups.add(group0);
        segment.put("groups", groups);
        segments.add(segment);
        layout.put("segments", segments);
        MemoryView.put("layout", layout);
        CorfuDBView v = new CorfuDBView(MemoryView);
        cm.forceNewView(v);
        cdr.invalidateViewAndWait(null);

        /* Make sure that second logunit is now part of the configuration */
        assertThat(cdr.getView().getSegments().get(0).getGroups().get(0).size())
                .isEqualTo(2);

        /* Make sure we can write to this configuration */
        IStreamingSequencer s = new StreamingSequencer(cdr);
        IWriteOnceAddressSpace woas = new WriteOnceAddressSpace(cdr);

        long seq = s.getNext();
        woas.write(seq, "hello world".getBytes());

        assertThat(woas.read(seq))
                .isEqualTo("hello world".getBytes());

        IWriteOnceLogUnit LU1 = ((IWriteOnceLogUnit) cdr.getView().getSegments().get(0).getGroups().get(0).get(0));
        IWriteOnceLogUnit LU2 = ((IWriteOnceLogUnit) cdr.getView().getSegments().get(0).getGroups().get(0).get(1));

        assertThat(LU1.read(0))
                .isEqualTo("hello world".getBytes());
        assertThat(LU2.read(0))
                .isEqualTo("hello world".getBytes());

        /* Fail LU2 */
        LU2.simulateFailure(true);

        /* Try writing again */
        seq = s.getNext();
        woas.write(seq, "hello world 2".getBytes());

        /* Can we successfully read? */
        assertThat(woas.read(0))
                .isEqualTo("hello world".getBytes());
        assertThat(woas.read(1))
                .isEqualTo("hello world 2".getBytes());

        /* try writing from the wrong epoch to LU1 */
        seq = s.getNext();
        ((CorfuDBSimpleLogUnitProtocol)LU1).epoch = cdr.getView().getEpoch()-1;
        assertRaises(() -> LU1.write(2, "hello world 3".getBytes()), NetworkException.class);
        ((CorfuDBSimpleLogUnitProtocol)LU1).epoch = cdr.getView().getEpoch();

        /* Un-fail LU2 */
        LU2.simulateFailure(false);

        /* Restore the original view */
        oldView.resetEpoch(cdr.getView().getEpoch() + 1);
        cm.forceNewView(oldView);
        cdr.invalidateViewAndWait(null);

        /* Make sure that old view only has a single log unit*/
        assertThat(cdr.getView().getSegments().get(0).getGroups().get(0).size())
                .isEqualTo(1);
    }

    @Test
    public void CorfuSequencerFailoverTest() throws Exception {
        CorfuDBRuntime cdr = new CorfuDBRuntime("http://localhost:12700/corfu");
        cdr.waitForViewReady();
        IConfigurationMaster cm = new ConfigurationMaster(cdr);
        cm.resetAll();

        CorfuDBView oldView = cdr.getView();

        /* Forcibly install a dual logunit configuration. */
        HashMap<String, Object> MemoryView = new HashMap<String, Object>();
        Long epoch = cdr.getView().getEpoch() + 1;
        MemoryView.put("epoch", epoch);

        MemoryView.put("logid", cdr.getView().getUUID().toString());
        MemoryView.put("pagesize", 4096);

        LinkedList<String> configMasters = new LinkedList<String>();
        configMasters.push("cdbcm://localhost:12700");
        MemoryView.put("configmasters", configMasters);

        LinkedList<String> sequencers = new LinkedList<String>();
        sequencers.push("cdbsts://localhost:12600");
        sequencers.push("cdbsts://localhost:12601");
        MemoryView.put("sequencers", sequencers);

        HashMap<String, Object> layout = new HashMap<String, Object>();
        LinkedList<HashMap<String, Object>> segments = new LinkedList<HashMap<String, Object>>();
        HashMap<String, Object> segment = new HashMap<String, Object>();
        segment.put("start", 0L);
        segment.put("sealed", 0L);
        LinkedList<HashMap<String, Object>> groups = new LinkedList<HashMap<String, Object>>();
        HashMap<String, Object> group0 = new HashMap<String, Object>();
        LinkedList<String> group0nodes = new LinkedList<String>();
        group0nodes.add("cdbslu://localhost:12800");
        group0.put("nodes", group0nodes);
        groups.add(group0);
        segment.put("groups", groups);
        segments.add(segment);
        layout.put("segments", segments);
        MemoryView.put("layout", layout);
        CorfuDBView v = new CorfuDBView(MemoryView);
        cm.forceNewView(v);
        cdr.invalidateViewAndWait(null);

        /* Make sure that second sequencer is now part of the configuration */
        assertThat(cdr.getView().getSequencers().size())
                .isEqualTo(2);

        /* Make sure we can write to this configuration */
        IStreamingSequencer s = new StreamingSequencer(cdr);
        IWriteOnceAddressSpace woas = new WriteOnceAddressSpace(cdr);

        long seq = s.getNext();
        woas.write(seq, "hello world".getBytes());

        IStreamSequencer S1 = (IStreamSequencer) cdr.getView().getSequencers().get(0);
        IStreamSequencer S2 = (IStreamSequencer) cdr.getView().getSequencers().get(1);

        //force failure
        S2.simulateFailure(true);

        /* Restore the original view */
        oldView.resetEpoch(cdr.getView().getEpoch() + 1);
        cm.forceNewView(oldView);
        cdr.invalidateViewAndWait(null);

        /* Make sure that old view only has a single log sequencer*/
        assertThat(cdr.getView().getSequencers().size())
                .isEqualTo(1);
    }
}

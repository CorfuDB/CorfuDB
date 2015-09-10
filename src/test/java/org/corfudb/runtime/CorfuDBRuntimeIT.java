package org.corfudb.runtime;

import org.corfudb.infrastructure.SimpleLogUnitServer;
import org.corfudb.infrastructure.StreamingSequencerServer;
import org.corfudb.runtime.protocols.logunits.CorfuDBSimpleLogUnitProtocol;
import org.corfudb.runtime.protocols.logunits.IWriteOnceLogUnit;
import org.corfudb.runtime.protocols.sequencers.ISimpleSequencer;
import org.corfudb.runtime.view.*;
import org.corfudb.util.CorfuInfrastructureBuilder;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.github.marschall.junitlambda.LambdaAssert.assertRaises;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 5/18/15.
 */
public class CorfuDBRuntimeIT {

    static UUID uuid = UUID.randomUUID();
    static Map<String, Object> luConfigMap = new HashMap<String,Object>() {
        {
            put("capacity", 200000);
            put("ramdisk", true);
            put("pagesize", 4096);
            put("trim", 0);
        }
    };

    static CorfuInfrastructureBuilder infrastructure =
            CorfuInfrastructureBuilder.getBuilder()
                    .addSequencer(9201, StreamingSequencerServer.class, "cdbsts", null)
                    .addLoggingUnit(9200, 0, SimpleLogUnitServer.class, "cdbslu", luConfigMap)
                    .start(9202);

    @Test
    public void isCorfuViewAccessible()
    {
        CorfuDBRuntime cdr = CorfuDBRuntime.createRuntime(infrastructure.getConfigString());
        cdr.waitForViewReady();
        assertThat(cdr.getView())
                .isNotNull();
    }


    @Test
    public void isCorfuViewUsable() throws Exception
    {
        CorfuDBRuntime cdr = CorfuDBRuntime.createRuntime(infrastructure.getConfigString());

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
        CorfuDBRuntime cdr = CorfuDBRuntime.createRuntime(infrastructure.getConfigString());
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

//    @Test
    public void CorfuLogunitFailoverTest() throws Exception {
        CorfuDBRuntime cdr = CorfuDBRuntime.createRuntime("http://localhost:12700/corfu");
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
        segment.put("replication", "cdbcr");
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

        assertThat(LU1.read(0, uuid))
                .isEqualTo("hello world".getBytes());
        assertThat(LU2.read(0, uuid))
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
        assertRaises(() -> LU1.write(2, null, "hello world 3".getBytes()), NetworkException.class);
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

    //@Test
    public void CorfuSequencerFailoverTest() throws Exception {
        CorfuDBRuntime cdr = CorfuDBRuntime.createRuntime("http://localhost:12700/corfu");
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
        segment.put("replication", "cdbcr");
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

        ISimpleSequencer S1 = (ISimpleSequencer) cdr.getView().getSequencers().get(0);
        ISimpleSequencer S2 = (ISimpleSequencer) cdr.getView().getSequencers().get(1);

        long seq = s.getNext();
        woas.write(seq, "hello world".getBytes());

        seq = s.getNext();
        woas.write(seq, "hello world 2".getBytes());


        assertThat(S1.sequenceGetCurrent())
                .isEqualTo(seq + 1);
        //force failure
        S1.simulateFailure(true);
        assertThat(S1.ping())
                .isEqualTo(false);

        //try to get another sequence number
        long seq2 = s.getNext();
        assertThat(seq2)
                .isGreaterThan(seq);

        assertThat(s.getNext())
                .isGreaterThan(seq2);

        S1.simulateFailure(false);
        assertThat(S1.ping())
                .isEqualTo(true);

        /* Restore the original view */
        oldView.resetEpoch(cdr.getView().getEpoch() + 1);
        cm.forceNewView(oldView);
        cdr.invalidateViewAndWait(null);

        /* Make sure that old view only has a single log sequencer*/
        assertThat(cdr.getView().getSequencers().size())
                .isEqualTo(1);
    }


  //  @Test
    public void viewFailureDoesNotLoop() throws Exception {
        CorfuDBRuntime cdr = CorfuDBRuntime.createRuntime("http://localhost:12700/corfu");
        cdr.waitForViewReady();
        IConfigurationMaster cm = new ConfigurationMaster(cdr);
        cm.resetAll();


        ISimpleSequencer S1 = (ISimpleSequencer) cdr.getView().getSequencers().get(0);
        S1.simulateFailure(true);

        CompletableFuture<Long> v = CompletableFuture.supplyAsync(
                () -> {
                    IStreamingSequencer s = new StreamingSequencer(cdr);
                    s.getNext();
                    return s.getNext();
                }
        );

        Thread.sleep(1000);
        S1.simulateFailure(false);
        Thread.sleep(1000);

        assertThat(v.join())
            .isEqualTo(1);
    }
}

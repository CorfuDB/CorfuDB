package org.corfudb.runtime;

import org.corfudb.runtime.view.*;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.UUID;

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
    public void CorfuFailoverTest() throws Exception
    {
        CorfuDBRuntime cdr = new CorfuDBRuntime("http://localhost:12700/corfu");
        cdr.waitForViewReady();
        IConfigurationMaster cm = new ConfigurationMaster(cdr);
        cm.resetAll();

        /* Forcibly install a dual logunit configuration. */
        HashMap<String, Object> MemoryView = new HashMap<String, Object>();
        MemoryView.put("epoch", 0L);

        MemoryView.put("logid", UUID.randomUUID().toString());
        MemoryView.put("pagesize", 4096);

        LinkedList<String> configMasters = new LinkedList<String>();
        configMasters.push("cdbcm://localhost:12700");
        MemoryView.put("configmasters", configMasters);

        LinkedList<String> sequencers = new LinkedList<String>();
        sequencers.push("cdbss://localhost:12600");
        MemoryView.put("sequencers", sequencers);

        HashMap<String,Object> layout = new HashMap<String,Object>();
        LinkedList<HashMap<String,Object>> segments = new LinkedList<HashMap<String,Object>>();
        HashMap<String,Object> segment = new HashMap<String,Object>();
        segment.put("start", 0L);
        segment.put("sealed", 0L);
        LinkedList<HashMap<String,Object>> groups = new LinkedList<HashMap<String,Object>>();
        HashMap<String,Object> group0 = new HashMap<String,Object>();
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
    }
}

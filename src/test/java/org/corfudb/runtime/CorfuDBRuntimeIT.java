package org.corfudb.runtime;

import org.corfudb.runtime.view.*;
import org.junit.Test;

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
}

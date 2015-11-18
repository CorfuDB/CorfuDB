package org.corfudb.runtime;

import org.corfudb.infrastructure.NettyLogUnitServer;
import org.corfudb.infrastructure.NettyStreamingSequencerServer;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.protocols.logunits.IWriteOnceLogUnit;
import org.corfudb.runtime.protocols.logunits.NettyLogUnitProtocol;
import org.corfudb.runtime.protocols.sequencers.ISimpleSequencer;
import org.corfudb.runtime.view.*;
import org.corfudb.util.CorfuITBuilder;
import org.junit.Test;

import javax.sound.midi.Sequencer;
import java.lang.reflect.InvocationTargetException;
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

    public static UUID uuid = UUID.randomUUID();
    public static Map<String, Object> luConfigMap = new HashMap<String,Object>() {
        {
            put("capacity", 200000);
            put("ramdisk", true);
            put("pagesize", 4096);
            put("trim", 0);
        }
    };

    public static CorfuDBView view =
            CorfuITBuilder.getBuilder()
                    .addSequencer(9201, NettyStreamingSequencerServer.class, "nsss", null)
                    .addLoggingUnit(9200, 0, NettyLogUnitServer.class, "nlu", luConfigMap)
                    .getView(9202)
                    .start();

    @Test
    public void isCorfuViewAccessible() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        LocalCorfuDBInstance cinstance = new LocalCorfuDBInstance(0);
        assertThat(cinstance.getViewJanitor())
                .isNotNull();
        assertThat(cinstance.getViewJanitor().isViewAccessible())
                .isNull();
    }

    public static LocalCorfuDBInstance generateInstance() {
        try {
            return new LocalCorfuDBInstance(0);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }
   //:q @Test
    public void isCorfuViewUsable() throws Exception
    {
        LocalCorfuDBInstance cinstance = new LocalCorfuDBInstance(0);
        IViewJanitor cm = cinstance.getViewJanitor();

        assertThat(cm)
                .isNotNull();
        assertThat(cm.isViewAccessible())
                .isNull();

        cm.resetAll();

        ISequencer s = cinstance.getSequencer();
        assertThat(s.getCurrent())
                .isEqualTo(0);

        IWriteOnceAddressSpace woas = cinstance.getAddressSpace();
        long addr = s.getNext();
        woas.write(addr, "hello world".getBytes());
        assertThat(woas.read(addr))
                .isEqualTo("hello world".getBytes());
    }

   // @Test
    public void isCorfuResettable() throws Exception
    {
        LocalCorfuDBInstance cinstance = new LocalCorfuDBInstance(0);
        IViewJanitor cm = cinstance.getViewJanitor();

        cm.resetAll();

        ISequencer s = cinstance.getSequencer();
        assertThat(s.getCurrent())
                .isEqualTo(0);
        s.getNext();
        cm.resetAll();
        assertThat(s.getCurrent())
                .isEqualTo(0);

        IWriteOnceAddressSpace woas = cinstance.getAddressSpace();
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

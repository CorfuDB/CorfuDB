package org.corfudb.runtime;

import org.corfudb.infrastructure.NettyLogUnitServer;
import org.corfudb.infrastructure.NettyStreamingSequencerServer;
<<<<<<< HEAD
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.protocols.logunits.IWriteOnceLogUnit;
import org.corfudb.runtime.protocols.logunits.NettyLogUnitProtocol;
import org.corfudb.runtime.protocols.sequencers.ISimpleSequencer;
=======
>>>>>>> some cleanups. there is a low-level bug preventing netty communication from going through
import org.corfudb.runtime.view.*;
import org.corfudb.util.CorfuITBuilder;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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
                    .addView(9202)
                    .start();

    @Test
    public void isCorfuViewAccessible() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        CorfuDBInstance cinstance = generateInstance();
        assertThat(cinstance.getViewJanitor())
                .isNotNull();
        assertThat(cinstance.getViewJanitor().isViewAccessible())
                .isNull();
    }

    public static CorfuDBInstance generateInstance() {
        try {
            return new CorfuDBInstance("localhost", 9202, view);
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
        CorfuDBInstance cinstance = generateInstance();
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
        CorfuDBInstance cinstance = generateInstance();
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

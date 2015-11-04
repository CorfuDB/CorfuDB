package org.corfudb.runtime;

import org.corfudb.infrastructure.NettyLogUnitServer;
import org.corfudb.infrastructure.NettyStreamingSequencerServer;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.protocols.logunits.IWriteOnceLogUnit;
import org.corfudb.runtime.protocols.logunits.NettyLogUnitProtocol;
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
                    .addSequencer(9201, NettyStreamingSequencerServer.class, "nsss", null)
                    .addLoggingUnit(9200, 0, NettyLogUnitServer.class, "nlu", luConfigMap)
                    .start(9202);

    @Test
    public void isCorfuViewAccessible()
    {
        CorfuDBRuntime cdr = CorfuDBRuntime.createRuntime(infrastructure.getConfigString());
        cdr.waitForViewReady();
        assertThat(cdr.getView())
                .isNotNull();
    }


}

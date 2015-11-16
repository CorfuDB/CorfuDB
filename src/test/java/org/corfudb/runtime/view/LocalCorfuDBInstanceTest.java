package org.corfudb.runtime.view;

import lombok.Getter;
import org.corfudb.infrastructure.NettyLogUnitServer;
import org.corfudb.infrastructure.NettyStreamingSequencerServer;
import org.corfudb.runtime.CorfuDBRuntimeIT;
import org.corfudb.runtime.collections.CDBSimpleMap;
import org.corfudb.util.RandomOpenPort;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 6/3/15.
 */
public class LocalCorfuDBInstanceTest extends ICorfuDBInstanceTest {

    @Getter
    ICorfuDBInstance instance;

    @Before
    public void setup()
    {
        instance = CorfuDBRuntimeIT.generateInstance();

        assertThat(instance)
                .isNotNull();
    }

    @Test
    public void canInstanceBeReset()
    {
        /* Generate a test stream */
        UUID streamID = UUID.randomUUID();
        CDBSimpleMap<String, String> map = instance.openObject(streamID, CDBSimpleMap.class);
        /* Insert a test item into the stream. */
        map.put("test", "helloword");
        /* reset the instance */
        instance.getViewJanitor().resetAll();
        instance.resetAllCaches();

        /* Re-open the test object, but under the same stream ID */
        map = instance.openObject(streamID, CDBSimpleMap.class);
        map.put("test2", "helloworld");

        /* There should be only one entry in the map. */
        assertThat(map.size())
                .isEqualTo(1);
    }


}

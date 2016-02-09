package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.LayoutRankMsg;
import org.corfudb.protocols.wireprotocol.LogUnitWriteMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.LogUnitServerAssertions.assertThat;

/**
 * Created by mwei on 2/4/16.
 */
public class LogUnitServerTest extends AbstractServerTest {

    @Override
    public IServer getDefaultServer() {
        return new LayoutServer(defaultOptionsMap());
    }

    @Test
    public void checkThatWritesArePersisted()
            throws Exception
    {
        String serviceDir = Files.createTempDir().getAbsolutePath();

        LogUnitServer s1 = new LogUnitServer(new ImmutableMap.Builder<String,Object>()
                .put("--log-path", serviceDir)
                .put("--memory", false)
                .put("--single", false)
                .put("--sync", true)
                .put("--max-cache", 1000000)
                .build());

        this.router.setServerUnderTest(s1);
        LogUnitWriteMsg m = new LogUnitWriteMsg(0L);
        //write at 0
        m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m.setRank(0L);
        m.setBackpointerMap(Collections.emptyMap());
        m.setPayload("0".getBytes());
        sendMessage(m);
        //100
        m = new LogUnitWriteMsg(100L);
        m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m.setRank(0L);
        m.setBackpointerMap(Collections.emptyMap());
        m.setPayload("100".getBytes());
        sendMessage(m);
        //and 10000000
        m = new LogUnitWriteMsg(10000000L);
        m.setAddress(10000000);
        m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m.setRank(0L);
        m.setBackpointerMap(Collections.emptyMap());
        m.setPayload("10000000".getBytes());
        sendMessage(m);

        assertThat(s1)
                .containsDataAtAddress(0)
                .containsDataAtAddress(100)
                .containsDataAtAddress(10000000);
        assertThat(s1)
                .matchesDataAtAddress(0, "0".getBytes())
                .matchesDataAtAddress(100, "100".getBytes())
                .matchesDataAtAddress(10000000, "10000000".getBytes());

        s1.shutdown();

        LogUnitServer s2 = new LogUnitServer(new ImmutableMap.Builder<String,Object>()
                .put("--log-path", serviceDir)
                .put("--single", false)
                .put("--memory", false)
                .put("--sync", true)
                .put("--max-cache", 1000000)
                .build());
        this.router.setServerUnderTest(s2);

        assertThat(s2)
                .containsDataAtAddress(0)
                .containsDataAtAddress(100)
                .containsDataAtAddress(10000000);
        assertThat(s2)
                .matchesDataAtAddress(0, "0".getBytes())
                .matchesDataAtAddress(100, "100".getBytes())
                .matchesDataAtAddress(10000000, "10000000".getBytes());
    }
}


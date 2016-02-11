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
        String serviceDir = getTempDir();

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

    @Test
    public void checkThatContiguousStreamIsCorrectlyCalculated()
            throws Exception
    {
        LogUnitServer s1 = new LogUnitServer(new ImmutableMap.Builder<String,Object>()
                .put("--memory", true)
                .put("--single", false)
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
        s1.compactTail();
        assertThat(s1)
                .hasContiguousStreamEntryAt(CorfuRuntime.getStreamID("a"), 0L);

        m = new LogUnitWriteMsg(1L);
        m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("b")));
        m.setRank(0L);
        m.setBackpointerMap(Collections.emptyMap());
        m.setPayload("1".getBytes());
        sendMessage(m);
        s1.compactTail();
        assertThat(s1)
                .hasContiguousStreamEntryAt(CorfuRuntime.getStreamID("a"), 0L);
        assertThat(s1)
                .hasContiguousStreamEntryAt(CorfuRuntime.getStreamID("b"), 1L);

        m = new LogUnitWriteMsg(2L);
        m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m.setRank(0L);
        m.setBackpointerMap(Collections.emptyMap());
        m.setPayload("10000000".getBytes());
        sendMessage(m);
        s1.compactTail();
        m = new LogUnitWriteMsg(100L);
        m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m.setRank(0L);
        m.setBackpointerMap(Collections.emptyMap());
        m.setPayload("10000000".getBytes());
        sendMessage(m);
        s1.compactTail();

        assertThat(s1)
                .hasContiguousStreamEntryAt(CorfuRuntime.getStreamID("a"), 0L);
        assertThat(s1)
                .hasContiguousStreamEntryAt(CorfuRuntime.getStreamID("b"), 1L);
        assertThat(s1)
                .hasContiguousStreamEntryAt(CorfuRuntime.getStreamID("a"), 2L);
        assertThat(s1)
                .doestNotHaveContiguousStreamEntryAt(CorfuRuntime.getStreamID("a"), 100L);
        s1.shutdown();
    }

    @Test
    public void checkThatContiguousTailIsCorrectlyCalculated()
            throws Exception
    {
        LogUnitServer s1 = new LogUnitServer(new ImmutableMap.Builder<String,Object>()
                .put("--memory", true)
                .put("--single", false)
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
        s1.compactTail();
        assertThat(s1)
                .hasContiguousTailAt(0L);

        m = new LogUnitWriteMsg(1L);
        m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m.setRank(0L);
        m.setBackpointerMap(Collections.emptyMap());
        m.setPayload("1".getBytes());
        sendMessage(m);
        s1.compactTail();
        assertThat(s1)
                .hasContiguousTailAt(1L);

        m = new LogUnitWriteMsg(100L);
        m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m.setRank(0L);
        m.setBackpointerMap(Collections.emptyMap());
        m.setPayload("10000000".getBytes());
        sendMessage(m);
        s1.compactTail();
        assertThat(s1)
                .hasContiguousTailAt(1L);

        s1.shutdown();
    }
}


package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.LoadingCache;
import org.corfudb.infrastructure.log.LogUnitEntry;
import org.corfudb.protocols.wireprotocol.LogUnitWriteMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.LogUnitServerAssertions.assertThat;

/**
 * Created by mwei on 2/4/16.
 */
public class LogUnitServerTest extends AbstractServerTest {

    @Override
    public AbstractServer getDefaultServer() {
        return new LogUnitServer(new ServerContextBuilder().build());
    }

    @Test
    public void checkHeapLeak() throws Exception {

        LogUnitServer s1 = new LogUnitServer(ServerContextBuilder.emptyContext());

        this.router.reset();
        this.router.addServer(s1);
        long address = 0L;
        LogUnitWriteMsg m = new LogUnitWriteMsg(address);
        //write at 0
        m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m.setRank(0L);
        m.setBackpointerMap(Collections.emptyMap());
        byte[] payload = "0".getBytes();
        m.setPayload(payload);
        sendMessage(m);

        LoadingCache<Long, LogUnitEntry> dataCache = s1.getDataCache();
        // Make sure that extra bytes are truncated from the payload byte buf
        assertThat(dataCache.get(address).getBuffer().capacity()).isEqualTo(payload.length);

    }

    @Test
    public void checkThatWritesArePersisted()
            throws Exception {
        String serviceDir = getTempDir();

        LogUnitServer s1 = new LogUnitServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .setSync(true)
                .build());

        this.router.reset();
        this.router.addServer(s1);
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

        LogUnitServer s2 = new LogUnitServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .setSync(true)
                .build());
        this.router.reset();
        this.router.addServer(s2);

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


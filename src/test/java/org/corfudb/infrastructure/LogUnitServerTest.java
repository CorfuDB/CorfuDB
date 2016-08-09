package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.corfudb.infrastructure.log.LogAddress;
import org.corfudb.infrastructure.log.LogUnitEntry;
import org.corfudb.protocols.wireprotocol.*;
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
        ByteBuf b = ByteBufAllocator.DEFAULT.buffer();
        b.writeByte(42);
        WriteRequest wr = WriteRequest.builder()
                            .writeMode(WriteMode.NORMAL)
                            .globalAddress(address)
                            .dataBuffer(b)
                            .build();
        //write at 0
        wr.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        wr.setRank(0L);
        wr.setBackpointerMap(Collections.emptyMap());

        sendMessage(CorfuMsgType.WRITE.payloadMsg(wr));

        LoadingCache<LogAddress, LogUnitEntry> dataCache = s1.getDataCache();
        // Make sure that extra bytes are truncated from the payload byte buf
        assertThat(dataCache.get(new LogAddress(address, null)).getBuffer().capacity()).isEqualTo(1);
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
        //write at 0
        ByteBuf b = ByteBufAllocator.DEFAULT.buffer();
        b.writeBytes("0".getBytes());
        WriteRequest m = WriteRequest.builder()
                .writeMode(WriteMode.NORMAL)
                .globalAddress(0L)
                .dataBuffer(b)
                .build();
        m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m.setRank(0L);
        m.setBackpointerMap(Collections.emptyMap());
        sendMessage(CorfuMsgType.WRITE.payloadMsg(m));
        //100
        b = ByteBufAllocator.DEFAULT.buffer();
        b.writeBytes("100".getBytes());
        m = WriteRequest.builder()
                .writeMode(WriteMode.NORMAL)
                .globalAddress(100L)
                .dataBuffer(b)
                .build();
        m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m.setRank(0L);
        m.setBackpointerMap(Collections.emptyMap());
        sendMessage(CorfuMsgType.WRITE.payloadMsg(m));
        //and 10000000
        b = ByteBufAllocator.DEFAULT.buffer();
        b.writeBytes("10000000".getBytes());
        m = WriteRequest.builder()
                .writeMode(WriteMode.NORMAL)
                .globalAddress(10000000L)
                .dataBuffer(b)
                .build();
        m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m.setRank(0L);
        m.setBackpointerMap(Collections.emptyMap());
        sendMessage(CorfuMsgType.WRITE.payloadMsg(m));

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


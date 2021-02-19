package org.corfudb.protocols.wireprotocol;

import static org.assertj.core.api.Java6Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.corfudb.protocols.CorfuProtocolCommon;
import org.corfudb.protocols.logprotocol.CheckpointEntry.CheckpointEntryType;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

public class PayloadTest {

    @Test
    public void checkConstructorMap() {
        List<Class<?>> types = Arrays.asList(
                Byte.class, Integer.class, Long.class, Boolean.class, Double.class, Float.class, String.class,
                Layout.class, CheckpointEntryType.class, UUID.class, byte[].class, ByteBuf.class
        );

        assertThat(CorfuProtocolCommon.getConstructorMap().keySet()).containsAll(types);
    }

    @Test
    public void testBuildPayloadFromBuffer(){
        final int value = 12345;
        ByteBuf payload = Unpooled.buffer().writeInt(value);
        Integer result = CorfuProtocolCommon.fromBuffer(payload, Integer.class);

        assertThat(result).isEqualTo(value);
    }

    @Test
    public void testSerialize(){
        ByteBuf buf = Unpooled.buffer();

        Set<String> payload = new HashSet<>();
        payload.add("value1");
        payload.add("value2");

        CorfuProtocolCommon.serialize(buf, payload);
        assertThat(CorfuProtocolCommon.setFromBuffer(buf, String.class)).isEqualTo(payload);
    }
}

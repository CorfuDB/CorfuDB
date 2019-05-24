package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.protocols.logprotocol.CheckpointEntry.CheckpointEntryType;
import org.corfudb.protocols.wireprotocol.IMetadata.DataRank;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import java.util.*;

import static org.assertj.core.api.Java6Assertions.assertThat;


public class ICorfuPayloadTest {

    @Test
    public void checkConstructorMap() {
        List<Class<?>> types = Arrays.asList(
                Byte.class, Integer.class, Long.class, Boolean.class, Double.class, Float.class, String.class,
                Layout.class, DataRank.class, CheckpointEntryType.class, UUID.class, byte[].class, ByteBuf.class
        );

        assertThat(ICorfuPayload.constructorMap.keySet()).containsAll(types);
    }

    @Test
    public void testBuildPayloadFromBuffer() {
        final int value = 12345;
        ByteBuf payload = Unpooled.buffer().writeInt(value);
        Integer result = ICorfuPayload.fromBuffer(payload, Integer.class);

        assertThat(result).isEqualTo(value);
    }

    @Test
    public void testSerialize() {
        ByteBuf buf = Unpooled.buffer();

        Set<String> payload = new HashSet<>();
        payload.add("value1");
        payload.add("value2");

        ICorfuPayload.serialize(buf, payload);

        assertThat(ICorfuPayload.setFromBuffer(buf, String.class)).isEqualTo(payload);
    }

    @Test
    public void testListSerialize() {
        ByteBuf buf = Unpooled.buffer();

        List<String> payload = new ArrayList<>();
        payload.add("value1");
        payload.add("value2");

        ICorfuPayload.serialize(buf, payload);

        List<String> deserializedList = ICorfuPayload.fromBuffer(buf, new TypeToken<List<String>>(){});

        assertThat(deserializedList).isEqualTo(payload);
    }

    @Test
    public void testNestedContainers() {
        ByteBuf buf = Unpooled.buffer();

        Set<List<String>> payload = new HashSet<>();
        List<String> list1 = Arrays.asList("value1", "value2");
        List<String> list2 = Arrays.asList("value3", "value4");

        payload.add(list1);
        payload.add(list2);

        ICorfuPayload.serialize(buf, payload);

        assertThat(ICorfuPayload.fromBuffer(buf, new TypeToken<Set<List<String>>>(){})).isEqualTo(payload);
    }

}
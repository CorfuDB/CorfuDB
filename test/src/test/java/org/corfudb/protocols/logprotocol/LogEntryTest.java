package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.CustomSerializer;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LogEntryTest {

    @Test
    public void testLazyDeserialization() {
        // This tests exercises the lazy deserialization functionality provided by
        // MultiObjectSMREntry. It creates an entry with two streams, their updates
        // are serialized with two different serializers. Only one of the serializers
        // registered. 

        // Create two updates on two different streams with different serializers
        SMRLogEntry smrLogEntry = new SMRLogEntry();

        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        SMRRecord update1 = new SMRRecord("method1", new Object[]{"arg1"}, Serializers.PRIMITIVE);

        ISerializer customSerializer = new CustomSerializer((byte) (Serializers.SYSTEM_SERIALIZERS_COUNT + 2));

        SMRRecord update2 = new SMRRecord("method2", new Object[]{"arg1"}, customSerializer);

        smrLogEntry.addTo(id1, update1);
        smrLogEntry.addTo(id2, update2);

        final long entryAddress = 5;
        smrLogEntry.setGlobalAddress(entryAddress);

        // Serialize smrLogEntry and deserialize it into a new object
        ByteBuf buf = Unpooled.buffer();
        smrLogEntry.serialize(buf);

        SMRLogEntry deserializedEntry = (SMRLogEntry) LogEntry.deserialize(buf, null);
        deserializedEntry.setGlobalAddress(entryAddress);

        // Verify that the buffer cache contains both streams
        assertThat(deserializedEntry.getStreamBuffers()).containsOnlyKeys(id1, id2);

        // Get updates for stream1 to force deserializing only one stream and verify
        // that the updates for stream two are not deserialized
        List<SMRRecord> stream1Updates = deserializedEntry.getSMRUpdates(id1);
        assertThat(stream1Updates).containsExactly(update1);
        stream1Updates.forEach(entry -> assertThat(entry.getGlobalAddress()).isEqualTo(entryAddress));

        assertThat(deserializedEntry.getStreamBuffers()).containsOnlyKeys(id2);
        assertThat(deserializedEntry.getStreamUpdates()).containsOnlyKeys(id1);

        // Verify that attempting to deserialize stream2's updates results in an
        // exception (since the serializer isn't registered)
        assertThatThrownBy(() -> deserializedEntry.getSMRUpdates(id2)).isInstanceOf(SerializerException.class);
        assertThatThrownBy(deserializedEntry::getEntryMap).isInstanceOf(SerializerException.class);
        assertThat(deserializedEntry.getStreamBuffers()).containsOnlyKeys(id2);
        assertThat(deserializedEntry.getStreamUpdates()).containsOnlyKeys(id1);

        // Register stream2's serializer and verify that its updates can be retrieved
        Serializers.registerSerializer(customSerializer);

        List<SMRRecord> stream2Updates = deserializedEntry.getSMRUpdates(id2);
        assertThat(stream2Updates).containsExactly(update2);
        stream2Updates.forEach(entry -> assertThat(entry.getGlobalAddress()).isEqualTo(entryAddress));

        assertThat(deserializedEntry.getStreamBuffers()).isEmpty();
        assertThat(deserializedEntry.getStreamUpdates()).containsOnlyKeys(id1, id2);

        // Verify that getEntryMap returns all updates and that the MultiSMREntry
        // structure is correct
        Map<UUID, List<SMRRecord>> allUpdates = deserializedEntry.getEntryMap();
        assertThat(allUpdates).containsOnlyKeys(id1, id2);

        List<SMRRecord> multiSMREntry1 = allUpdates.get(id1);
        assertThat(multiSMREntry1).isEqualTo(Collections.singletonList(update1));
        multiSMREntry1.forEach(record -> assertThat(record.getGlobalAddress()).isEqualTo(entryAddress));

        List<SMRRecord> multiSMREntry2 = allUpdates.get(id2);
        assertThat(multiSMREntry2).isEqualTo(Collections.singletonList(update2));
        multiSMREntry2.forEach(entry -> assertThat(entry.getGlobalAddress()).isEqualTo(entryAddress));
    }
}

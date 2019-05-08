package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.CustomSerializer;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LogEntryTest {

    @Test
    public void seekToEndSMREntry () {
        // Create a buffer with two serialized SMR entries
        // skip the first entry (with seekToEnd) and attempt to deserialize the
        // rest of the buffer, which should yield the second SMR entry
        SMREntry smr1 = new SMREntry("method", new Object[]{}, Serializers.PRIMITIVE);
        SMREntry smr2 = new SMREntry("method", new Object[]{"arg1"}, Serializers.PRIMITIVE);

        ByteBuf buf = Unpooled.buffer();

        Serializers.CORFU.serialize(smr1, buf);
        Serializers.CORFU.serialize(smr2, buf);

        SMREntry.seekToEnd(buf);
        SMREntry recoveredEntry = (SMREntry) Serializers.CORFU.deserialize(buf);
        assertThat(recoveredEntry).isEqualTo(smr2);
        buf.resetReaderIndex();
        recoveredEntry = (SMREntry) Serializers.CORFU.deserialize(buf);
        assertThat(recoveredEntry).isEqualTo(smr1);

        // Verify that both entries can be skipped
        buf.resetReaderIndex();
        SMREntry.seekToEnd(buf);
        SMREntry.seekToEnd(buf);
        assertThat(buf.readerIndex()).isEqualTo(buf.writerIndex());
    }

    @Test
    public void seekToEndMultiSMREntry() {
        // Create a buffer with two serialized MultiSMR entries
        // skip the first entry (with seekToEnd) and attempt to deserialize the
        // rest of the buffer, which should yield the second MultiSMR entry
        SMREntry smr1 = new SMREntry("method", new Object[]{}, Serializers.PRIMITIVE);
        SMREntry smr2 = new SMREntry("method", new Object[]{"arg1"}, Serializers.PRIMITIVE);
        MultiSMREntry multiSMR1 = new MultiSMREntry();
        MultiSMREntry multiSMR2 = new MultiSMREntry();

        multiSMR1.addTo(smr1);
        multiSMR1.addTo(smr1);
        multiSMR2.addTo(smr2);

        ByteBuf buf = Unpooled.buffer();

        Serializers.CORFU.serialize(multiSMR1, buf);
        Serializers.CORFU.serialize(multiSMR2, buf);

        MultiSMREntry.seekToEnd(buf);
        MultiSMREntry recoveredMultiSMR = (MultiSMREntry) Serializers.CORFU.deserialize(buf);
        assertThat(recoveredMultiSMR).isEqualTo(multiSMR2);
        buf.resetReaderIndex();
        recoveredMultiSMR = (MultiSMREntry) Serializers.CORFU.deserialize(buf);
        assertThat(recoveredMultiSMR).isEqualTo(multiSMR1);

        // Verify that both entries can be skipped
        buf.resetReaderIndex();
        MultiSMREntry.seekToEnd(buf);
        MultiSMREntry.seekToEnd(buf);
        assertThat(buf.readerIndex()).isEqualTo(buf.writerIndex());
    }

    @Test
    public void testLazyDeserialization() {
        // This tests exercises the lazy deserialization functionality provided by
        // MultiObjectSMREntry. It creates an entry with two streams, their updates
        // are serialized with two different serializers. Only one of the serializers
        // registered. 

        // Create two updates on two different streams with different serializers
        MultiObjectSMREntry multiObjSmrEntry = new MultiObjectSMREntry();

        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        SMREntry update1 = new SMREntry("method1", new Object[]{"arg1"}, Serializers.PRIMITIVE);

        ISerializer customSerializer = new CustomSerializer((byte)(Serializers.SYSTEM_SERIALIZERS_COUNT + 2));

        SMREntry update2 = new SMREntry("method2", new Object[]{"arg1"}, customSerializer);

        multiObjSmrEntry.addTo(id1, update1);
        multiObjSmrEntry.addTo(id2, update2);

        final long entryAddress = 5;

        // Serialize MultiObjectSMREntry and deserialize it into a new object
        ByteBuf buf = Unpooled.buffer();
        multiObjSmrEntry.serialize(buf);

        MultiObjectSMREntry deserializedEntry = (MultiObjectSMREntry) LogEntry.deserialize(buf);
        deserializedEntry.setGlobalAddress(entryAddress);

        // Verify that the buffer cache contains both streams
        assertThat(deserializedEntry.getStreamBuffers()).containsOnlyKeys(id1, id2);

        // Get updates for stream1 to force deserializing only one stream and verify
        // that the updates for stream two are not deserialized
        List<SMREntry> stream1Updates = deserializedEntry.getSMRUpdates(id1);
        assertThat(stream1Updates).containsExactly(update1);
        stream1Updates.stream().forEach(entry -> assertThat(entry.getGlobalAddress()).isEqualTo(entryAddress));

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

        List<SMREntry> stream2Updates = deserializedEntry.getSMRUpdates(id2);
        assertThat(stream2Updates).containsExactly(update2);
        stream2Updates.stream().forEach(entry -> assertThat(entry.getGlobalAddress()).isEqualTo(entryAddress));

        assertThat(deserializedEntry.getStreamBuffers()).isEmpty();
        assertThat(deserializedEntry.getStreamUpdates()).containsOnlyKeys(id1, id2);

        // Verify that getEntryMap returns all updates and that the MultiSMREntry
        // structure is correct
        Map<UUID, MultiSMREntry> allUpdates = deserializedEntry.getEntryMap();
        assertThat(allUpdates).containsOnlyKeys(id1, id2);

        MultiSMREntry multiSMREntry1 = allUpdates.get(id1);
        assertThat(multiSMREntry1.getGlobalAddress()).isEqualTo(entryAddress);
        assertThat(multiSMREntry1.getUpdates()).isEqualTo(Arrays.asList(update1));
        multiSMREntry1.getUpdates().stream().forEach(entry -> assertThat(entry.getGlobalAddress())
                .isEqualTo(entryAddress));

        MultiSMREntry multiSMREntry2 = allUpdates.get(id2);
        assertThat(multiSMREntry2.getGlobalAddress()).isEqualTo(entryAddress);
        assertThat(multiSMREntry2.getUpdates()).isEqualTo(Arrays.asList(update2));
        multiSMREntry2.getUpdates().stream().forEach(entry -> assertThat(entry.getGlobalAddress())
                .isEqualTo(entryAddress));
    }
}

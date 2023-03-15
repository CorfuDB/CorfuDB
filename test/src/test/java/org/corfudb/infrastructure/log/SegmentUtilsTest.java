package org.corfudb.infrastructure.log;

import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.corfudb.infrastructure.log.SegmentUtils.getSegmentHeader;

public class SegmentUtilsTest {

    @Test
    public void tesSegmentHeader() throws Exception {
        final int version = 1;
        ByteBuffer buf = getSegmentHeader(version);
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);

        LogFormat.LogHeader header = LogFormat.LogHeader.parseFrom(bytes);
        assertThat(header.getVersion()).isEqualTo(version);
    }

    @Test
    public void logEntrySerializationTest() throws Exception {
        byte[] payload = "payload".getBytes();
        LogData ld = new LogData(DataType.DATA, payload);
        final long address = 1000L;
        ld.setGlobalAddress(address);
        UUID streamId = UUID.nameUUIDFromBytes("stream".getBytes());
        final long prevAddress = 500L;
        Map<UUID, Long> backpointerMap = Collections.singletonMap(streamId, prevAddress);
        ld.setBackpointerMap(backpointerMap);
        UUID clientId = UUID.nameUUIDFromBytes("client1".getBytes());
        ld.setClientId(clientId);

        // Serialize
        LogFormat.LogEntry entry = SegmentUtils.getLogEntry(address, ld);
        LogFormat.Metadata metadata = SegmentUtils.getMetadata(entry);
        ByteBuffer serializedBuf = SegmentUtils.getByteBuffer(metadata, entry);

        assertThat(serializedBuf.remaining())
                .isEqualTo(entry.getSerializedSize() + metadata.getSerializedSize());

        // Deserialize
        byte[] metadataBytes = new byte[metadata.getSerializedSize()];
        serializedBuf.get(metadataBytes);
        byte[] entryBytes = new byte[entry.getSerializedSize()];
        serializedBuf.get(entryBytes);

        LogFormat.Metadata deserializedMetadata = LogFormat.Metadata.parseFrom(metadataBytes);

        assertThat(deserializedMetadata.getLength())
                .isEqualTo(entry.getSerializedSize());

        assertThat(deserializedMetadata.getPayloadChecksum())
                .isEqualTo(metadata.getPayloadChecksum());

        LogFormat.LogEntry deserializedEntry = LogFormat.LogEntry.parseFrom(entryBytes);

        LogData deserializedLogData = SegmentUtils.getLogData(deserializedEntry);

        assertThat(ld).isEqualByComparingTo(deserializedLogData);
    }
}

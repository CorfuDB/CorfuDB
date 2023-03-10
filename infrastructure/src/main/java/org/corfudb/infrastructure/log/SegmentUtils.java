package org.corfudb.infrastructure.log;

import com.google.common.base.Preconditions;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import io.netty.buffer.Unpooled;
import org.corfudb.common.compression.Codec;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.wireprotocol.LogData;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.infrastructure.utils.Crc32c.getChecksum;

public final class SegmentUtils {

    private SegmentUtils() {}

    public static LogData getLogData(LogFormat.LogEntry entry) {
        ByteBuffer entryData = ByteBuffer.wrap(entry.getData().toByteArray());

        int ldCodecType = entry.hasCodecType() ? entry.getCodecType() : Codec.Type.NONE.getId();

        LogData logData = new LogData(org.corfudb.protocols.wireprotocol
                .DataType.typeMap.get((byte) entry.getDataType().getNumber()),
                Unpooled.wrappedBuffer(entryData.array()), ldCodecType);

        logData.setBackpointerMap(getUuidLongMap(entry.getBackpointersMap()));
        logData.setGlobalAddress(entry.getGlobalAddress());
        logData.setEpoch(entry.getEpoch());

        if (entry.hasThreadId()) {
            logData.setThreadId(entry.getThreadId());
        }
        if (entry.hasClientIdLeastSignificant() && entry.hasClientIdMostSignificant()) {
            long lsd = entry.getClientIdLeastSignificant();
            long msd = entry.getClientIdMostSignificant();
            logData.setClientId(new UUID(msd, lsd));
        }

        if (entry.hasCheckpointEntryType()) {
            logData.setCheckpointType(CheckpointEntry.CheckpointEntryType
                    .typeMap.get((byte) entry.getCheckpointEntryType().ordinal()));

            Preconditions.checkArgument(entry.hasCheckpointIdLeastSignificant());
            Preconditions.checkArgument(entry.hasCheckpointIdMostSignificant());

            long lsd = entry.getCheckpointIdLeastSignificant();
            long msd = entry.getCheckpointIdMostSignificant();
            UUID checkpointId = new UUID(msd, lsd);

            logData.setCheckpointId(checkpointId);

            lsd = entry.getCheckpointedStreamIdLeastSignificant();
            msd = entry.getCheckpointedStreamIdMostSignificant();
            UUID streamId = new UUID(msd, lsd);

            logData.setCheckpointedStreamId(streamId);

            logData.setCheckpointedStreamStartLogAddress(
                    entry.getCheckpointedStreamStartLogAddress());
        }

        return logData;
    }

    public static ByteBuffer getByteBufferWithMetaData(AbstractMessage message) {
        LogFormat.Metadata metadata = getMetadata(message);
        return getByteBuffer(metadata, message);
    }

    public static LogFormat.Metadata getMetadata(AbstractMessage message) {
        return LogFormat.Metadata.newBuilder()
                .setPayloadChecksum(getChecksum(message.toByteArray()))
                .setLengthChecksum(getChecksum(message.getSerializedSize()))
                .setLength(message.getSerializedSize())
                .build();
    }

    public static ByteBuffer getByteBuffer(LogFormat.Metadata metadata, AbstractMessage message) {
        ByteBuffer buf = ByteBuffer.allocate(metadata.getSerializedSize() + message.getSerializedSize());
        buf.put(metadata.toByteArray());
        buf.put(message.toByteArray());
        buf.flip();
        return buf;
    }

    public static ByteBuffer getSegmentHeader(int version) {
        LogFormat.LogHeader header = LogFormat.LogHeader.newBuilder()
                .setVersion(version)
                .setVerifyChecksum(true)
                .build();
        return getByteBufferWithMetaData(header);
    }

    public static LogFormat.LogEntry getLogEntry(long address, LogData entry) {
        ByteBuffer data = ByteBuffer.wrap(entry.getData() == null ? new byte[0] : entry.getData());

        LogFormat.LogEntry.Builder logEntryBuilder = LogFormat.LogEntry.newBuilder()
                .setDataType(LogFormat.DataType.forNumber(entry.getType().ordinal()))
                .setCodecType(entry.getPayloadCodecType().getId())
                .setData(ByteString.copyFrom(data))
                .setGlobalAddress(address)
                .setEpoch(entry.getEpoch())
                .addAllStreams(getStrUUID(entry.getStreams()))
                .putAllBackpointers(getStrLongMap(entry.getBackpointerMap()));

        if (entry.getClientId() != null && entry.getThreadId() != null) {
            logEntryBuilder.setClientIdMostSignificant(
                    entry.getClientId().getMostSignificantBits());
            logEntryBuilder.setClientIdLeastSignificant(
                    entry.getClientId().getLeastSignificantBits());
            logEntryBuilder.setThreadId(entry.getThreadId());
        }

        if (entry.hasCheckpointMetadata()) {
            logEntryBuilder.setCheckpointEntryType(
                    LogFormat.CheckpointEntryType.forNumber(
                            entry.getCheckpointType().ordinal()));
            logEntryBuilder.setCheckpointIdMostSignificant(
                    entry.getCheckpointId().getMostSignificantBits());
            logEntryBuilder.setCheckpointIdLeastSignificant(
                    entry.getCheckpointId().getLeastSignificantBits());
            logEntryBuilder.setCheckpointedStreamIdLeastSignificant(
                    entry.getCheckpointedStreamId().getLeastSignificantBits());
            logEntryBuilder.setCheckpointedStreamIdMostSignificant(
                    entry.getCheckpointedStreamId().getMostSignificantBits());
            logEntryBuilder.setCheckpointedStreamStartLogAddress(
                    entry.getCheckpointedStreamStartLogAddress());
        }

        return logEntryBuilder.build();
    }

    public static Map<String, Long> getStrLongMap(Map<UUID, Long> uuidLongMap) {
        Map<String, Long> stringLongMap = new HashMap<>();

        for (Map.Entry<UUID, Long> entry : uuidLongMap.entrySet()) {
            stringLongMap.put(entry.getKey().toString(), entry.getValue());
        }

        return stringLongMap;
    }

    @SuppressWarnings("checkstyle:abbreviationaswordinname")  // Due to deprecation
    private static Map<UUID, Long> getUuidLongMap(Map<String, Long> stringLongMap) {
        Map<UUID, Long> uuidLongMap = new HashMap<>();

        for (Map.Entry<String, Long> entry : stringLongMap.entrySet()) {
            uuidLongMap.put(UUID.fromString(entry.getKey()), entry.getValue());
        }

        return uuidLongMap;
    }

    @SuppressWarnings("checkstyle:abbreviationaswordinname") // Due to deprecation
    private static Set<String> getStrUUID(Set<UUID> uuids) {
        Set<String> strUUIds = new HashSet<>();

        for (UUID uuid : uuids) {
            strUUIds.add(uuid.toString());
        }

        return strUUIds;
    }

}

package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.corfudb.runtime.CorfuRuntime;


/**
 * Object & serialization methods for in-stream checkpoint
 * summarization of SMR object state.
 */
@ToString(callSuper = true)
@NoArgsConstructor
public class CheckpointEntry extends LogEntry {

    @RequiredArgsConstructor
    public enum CheckpointEntryType {
        START(0),           // Mandatory: 1st record in checkpoint
        CONTINUATION(1),    // Optional: 2nd through (n-1)th record
        END(2);             // Mandatory: final record checkpoint

        public final int type;

        public byte asByte() {
            return (byte) type;
        }

        public static final Map<Byte, CheckpointEntryType> typeMap =
                Arrays.stream(CheckpointEntryType.values())
                        .collect(Collectors.toMap(
                                CheckpointEntryType::asByte, Function.identity()));
    }

    @RequiredArgsConstructor
    public enum CheckpointDictKey {
        START_TIME(0),
        END_TIME(1),
        START_LOG_ADDRESS(2),
        ENTRY_COUNT(3),
        BYTE_COUNT(4),
        SNAPSHOT_ADDRESS(5);

        public final int type;

        public byte asByte() {
            return (byte) type;
        }

        public static final Map<Byte, CheckpointDictKey> typeMap =
                Arrays.stream(CheckpointDictKey.values())
                        .collect(Collectors.toMap(
                                CheckpointDictKey::asByte, Function.identity()));
    }

    /** Type of entry.
     */
    @Getter
    CheckpointEntryType cpType;

    /**
     * Unique identifier for this checkpoint.  All entries
     * for the same checkpoint state must use the same ID.
     */
    @Deprecated // TODO: Add replacement method that conforms to style
    @SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
    @Getter
    UUID checkpointId;

    @Getter
    UUID streamId;

    /** Author/cause/trigger of this checkpoint
     */
    @Deprecated // TODO: Add replacement method that conforms to style
    @SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
    @Getter
    String checkpointAuthorId;

    /** Map of checkpoint metadata, see key constants above.
     */
    @Deprecated // TODO: Add replacement method that conforms to style
    @SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
    @Getter
    Map<CheckpointDictKey, String> dict;

    /** Optional: SMREntry objects that contain SMR
     *  object state of the stream that we're checkpointing.
     *  May be present in any CheckpointEntryType, but typically
     *  used by CONTINUATION entries.
     */
    @Getter
    @Setter
    MultiSMREntry smrEntries;

    /** Byte count of smrEntries in serialized form, zero
     *  if smrEntries.size() is zero or if value is unknown.
     */
    @Getter
    int smrEntriesBytes = 0;

    public CheckpointEntry(CheckpointEntryType type, String authorId, UUID checkpointId,
                           UUID streamId, Map<CheckpointDictKey,String> dict, MultiSMREntry smrEntries) {
        super(LogEntryType.CHECKPOINT);
        this.cpType = type;
        this.checkpointId = checkpointId;
        this.streamId = streamId;
        this.checkpointAuthorId = authorId;
        this.dict = dict;
        this.smrEntries = smrEntries;
    }

    /**
     * This function provides the remaining buffer. Child entries
     * should initialize their contents based on the buffer.
     *
     * @param b The remaining buffer.
     * @param rt The CorfuRuntime used by the SMR object.
     * @return A CheckpointEntry.
     */
    @Override
    void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        super.deserializeBuffer(b, rt);
        cpType = CheckpointEntryType.typeMap.get(b.readByte());
        checkpointId = new UUID(b.readLong(), b.readLong());
        streamId = new UUID(b.readLong(), b.readLong());
        checkpointAuthorId = deserializeString(b);
        dict = new HashMap<>();
        short mapEntries = b.readShort();
        for (short i = 0; i < mapEntries; i++) {
            CheckpointDictKey k = CheckpointDictKey.typeMap.get(b.readByte());
            String v = deserializeString(b);
            dict.put(k, v);
        }
        smrEntries = null;
        byte hasSmrEntries = b.readByte();
        if (hasSmrEntries > 0) {
            smrEntries = (MultiSMREntry) MultiSMREntry.deserialize(b, runtime);
        }
        smrEntriesBytes = b.readInt();
    }

    /**
     * Serialize the given LogEntry into a given byte buffer.
     *
     * <p>NOTE: This method has a side-effect of updating the
     *          this.smrEntriesBytes field.
     *
     * @param b The buffer to serialize into.
     */
    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);

        if (cpType == CheckpointEntryType.END
                && getDict().get(CheckpointDictKey.START_LOG_ADDRESS) == null) {
            throw new IllegalArgumentException(dict.get(CheckpointDictKey.START_LOG_ADDRESS));
        }

        b.writeByte(cpType.asByte());
        b.writeLong(checkpointId.getMostSignificantBits());
        b.writeLong(checkpointId.getLeastSignificantBits());
        b.writeLong(streamId.getMostSignificantBits());
        b.writeLong(streamId.getLeastSignificantBits());
        serializeString(checkpointAuthorId, b);
        b.writeShort(dict == null ? 0 : dict.size());
        if (dict != null) {
            dict.entrySet().stream()
                    .forEach(x -> {
                        b.writeByte(x.getKey().asByte());
                        serializeString(x.getValue(), b);
                    });
        }
        if (smrEntries != null) {
            b.writeByte(1);
            int byteStart = b.readableBytes();
            smrEntries.serialize(b);
            smrEntriesBytes = b.readableBytes() - byteStart;
        } else {
            b.writeShort(0);
            smrEntriesBytes = 0;
        }
        b.writeInt(smrEntriesBytes);
    }

    /** Helper function to deserialize a String.
     *
     * @param b Source buffer
     * @return A String.
     */
    private String deserializeString(ByteBuf b) {
        short len = b.readShort();
        byte[] bytes = new byte[len];
        b.readBytes(bytes, 0, len);
        return new String(bytes);
    }

    /** Helper function to serialize a String.
     *
     * @param b Target buffer
     */
    private void serializeString(String s, ByteBuf b) {
        b.writeShort(s.length());
        b.writeBytes(s.getBytes());
    }
}

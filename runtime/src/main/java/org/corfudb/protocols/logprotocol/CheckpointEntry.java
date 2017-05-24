package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.corfudb.runtime.CorfuRuntime;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

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
                        .collect(Collectors.toMap(CheckpointEntryType::asByte, Function.identity()));
    };

    @RequiredArgsConstructor
    public enum CheckpointDictKey {
        START_TIME(0),
        END_TIME(1),
        START_LOG_ADDRESS(2),
        ENTRY_COUNT(3),
        BYTE_COUNT(4);

        public final int type;

        public byte asByte() {
            return (byte) type;
        }

        public static final Map<Byte, CheckpointDictKey> typeMap =
                Arrays.stream(CheckpointDictKey.values())
                        .collect(Collectors.toMap(CheckpointDictKey::asByte, Function.identity()));
    }

    /** Type of entry
     */
    @Getter
    CheckpointEntryType cpType;

    /**
     * Unique identifier for this checkpoint.  All entries
     * for the same checkpoint state must use the same ID.
     */
    @Getter
    UUID checkpointID;

    /** Author/cause/trigger of this checkpoint
     */
    @Getter
    String checkpointAuthorID;

    /** Map of checkpoint metadata, see key constants above
     */
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

    public CheckpointEntry(CheckpointEntryType type, String authorID, UUID checkpointID,
                           Map<CheckpointDictKey,String> dict, MultiSMREntry smrEntries) {
        super(LogEntryType.CHECKPOINT);
        this.cpType = type;
        this.checkpointID = checkpointID;
        this.checkpointAuthorID = authorID;
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
        checkpointID = new UUID(b.readLong(), b.readLong());
        checkpointAuthorID = deserializeString(b);
        dict = new HashMap<>();
        short mapEntries = b.readShort();
        for (short i = 0; i < mapEntries; i++) {
            CheckpointDictKey k = CheckpointDictKey.typeMap.get(b.readByte());
            String v = deserializeString(b);
            dict.put(k, v);
        }
        smrEntries = null;
        byte hasSMREntries = b.readByte();
        if (hasSMREntries > 0) {
            smrEntries = (MultiSMREntry) MultiSMREntry.deserialize(b, runtime);
        }
        smrEntriesBytes = b.readInt();
    }

    /**
     * Serialize the given LogEntry into a given byte buffer.
     *
     * NOTE: This method has a side-effect of updating the
     *       this.smrEntriesBytes field.
     *
     * @param b The buffer to serialize into.
     */
    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeByte(cpType.asByte());
        b.writeLong(checkpointID.getMostSignificantBits());
        b.writeLong(checkpointID.getLeastSignificantBits());
        serializeString(checkpointAuthorID, b);
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
     * @param b
     * @return A String.
     */
    private String deserializeString(ByteBuf b) {
        short len = b.readShort();
        byte bytes[] = new byte[len];
        b.readBytes(bytes, 0, len);
        return new String(bytes);
    }

    /** Helper function to serialize a String.
     *
     * @param b
     */
    private void serializeString(String s, ByteBuf b) {
        b.writeShort(s.length());
        b.writeBytes(s.getBytes());
    }
}

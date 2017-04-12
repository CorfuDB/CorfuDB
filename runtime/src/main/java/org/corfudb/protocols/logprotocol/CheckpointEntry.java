package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import lombok.*;
import org.corfudb.runtime.CorfuRuntime;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by sfritchie on 4/6/17.
 */
@ToString(callSuper = true)
@NoArgsConstructor
public class CheckpointEntry extends LogEntry {

    public static void dump(ByteBuf b) {
        byte[] bulk = new byte[b.readableBytes()];
        b.readBytes(bulk, 0, b.readableBytes() - 1);
        dump(bulk);
    }

    public static void dump(byte[] bulk) {
        if (bulk != null) {
            System.err.printf("Bulk(%d): ", bulk.length);
            for (int i = 0; i < bulk.length; i++) {
                System.err.printf("%d,", bulk[i]);
            }
            System.err.printf("\n");
        }
    }

    @RequiredArgsConstructor
    public enum CheckpointEntryType {
        START(1),           // Mandatory: 1st record in checkpoint
        CONTINUATION(2),    // Optional: 2nd through (n-1)th record
        END(3),             // Mandatory: for successful checkpoint
        FAIL(4);            // Optional: external party declares this checkpoint has failed

        public final int type;

        public byte asByte() {
            return (byte) type;
        }
    };

    @Getter
    CheckpointEntryType cpType;
    @Getter
    UUID checkpointID;  // Unique identifier for this checkpoint
    @Getter
    String checkpointAuthorID;  // TODO: UUID instead?
    @Getter
    Map<String, String> dict;
    @Getter
    byte[] bulk;

    public CheckpointEntry(CheckpointEntryType type, String authorID, UUID checkpointID,
                           Map<String,String> dict, ByteBuf bulk) {
        super(LogEntryType.CHECKPOINT);
        if (bulk == null) {
            constructorCommon(type, authorID, checkpointID, dict, null);
        } else {
            byte[] bulkBytes = new byte[bulk.readableBytes()];
            bulk.getBytes(0, bulkBytes);
            constructorCommon(type, authorID, checkpointID, dict, bulkBytes);
        }
    }

    public CheckpointEntry(CheckpointEntryType type, String authorID, UUID checkpointID,
                           Map<String,String> dict, byte[] bulk) {
        super(LogEntryType.CHECKPOINT);
        constructorCommon(type, authorID, checkpointID, dict, bulk);
    }

    private void constructorCommon(CheckpointEntryType type, String authorID, UUID checkpointID,
                                   Map<String,String> dict, byte[] bulk) {
        this.cpType = type;
        this.checkpointID = checkpointID;
        this.checkpointAuthorID = authorID;
        this.dict = dict;
        this.bulk = bulk;
        dump(bulk);
    }

    static final Map<Byte, CheckpointEntryType> typeMap =
            Arrays.stream(CheckpointEntryType.values())
                    .collect(Collectors.toMap(CheckpointEntryType::asByte, Function.identity()));

    /**
     * This function provides the remaining buffer. Child entries
     * should initialize their contents based on the buffer.
     *
     * @param b The remaining buffer.
     */
    @Override
    void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        super.deserializeBuffer(b, rt);
        cpType = typeMap.get(b.readByte());
        long cpidMSB = b.readLong();
        long cpidLSB = b.readLong();
        checkpointID = new UUID(cpidMSB, cpidLSB);
        checkpointAuthorID = deserializeString(b);
        dict = new HashMap<>();
        short mapEntries = b.readShort();
        for (short i = 0; i < mapEntries; i++) {
            String k = deserializeString(b);
            String v = deserializeString(b);
            dict.put(k, v);
        }
        int bulkLen = b.readInt();
        bulk = new byte[bulkLen];
        b.readBytes(bulk, 0, bulkLen);
    }

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
                        serializeString(x.getKey(), b);
                        serializeString(x.getValue(), b);
                    });
        }
        b.writeInt(bulk == null ? 0 : bulk.length);
        if (bulk != null) { b.writeBytes(bulk); }
    }

    private String deserializeString(ByteBuf b) {
        short len = b.readShort();
        byte bytes[] = new byte[len];
        b.readBytes(bytes, 0, len);
        return new String(bytes);
    }

    private void serializeString(String s, ByteBuf b) {
        b.writeShort(s.length());
        b.writeBytes(s.getBytes());
    }
}

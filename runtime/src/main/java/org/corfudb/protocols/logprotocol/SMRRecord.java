package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import java.util.Arrays;


/**
 * Created by mwei on 1/8/16.
 */
@SuppressWarnings("checkstyle:abbreviation")
@ToString
@EqualsAndHashCode(exclude = {"serializedSize"})
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SMRRecord {

    public final static SMRRecord COMPACTED_RECORD = SMRRecord.builder().build();
    public final static byte[] COMPACTED_RECORD_SERIALIZED;

    static {
        ByteBuf buf = Unpooled.buffer(1);
        COMPACTED_RECORD.serialize(buf);
        COMPACTED_RECORD_SERIALIZED = buf.array();
    }

    /**
     * The name of the SMR method. Note that this is limited to the size of a short.
     */
    @SuppressWarnings("checkstyle:MemberName")
    @Getter
    private String SMRMethod;

    /**
     * The arguments to the SMR method, which could be 0.
     */
    @SuppressWarnings("checkstyle:MemberName")
    @Getter
    private Object[] SMRArguments;

    /**
     * The serializer used to serialize the SMR arguments.
     */
    @Getter
    private ISerializer serializerType;

    /**
     * An undo record, which can be used to undo this method.
     */
    @Getter
    public transient Object undoRecord;

    /**
     * A flag indicating whether an undo record is present. Necessary
     * because undo records may be NULL.
     */
    @Getter
    public boolean undoable;

    /**
     * The upcall result, if present.
     */
    @Getter
    public transient Object upcallResult;

    /**
     * If there is an upcall result for this modification.
     */
    @Getter
    public transient boolean haveUpcallResult = false;

    /**
     * The locator of this instance of SMRRecord in global log.
     */
    @Getter
    @Setter
    public transient SMRRecordLocator locator = null;

    /**
     * The global address this record is associated to.
     */
    @Getter
    @Setter
    long globalAddress = Address.NON_ADDRESS;

    /**
     * The size in bytes of this SMREntry when serialized.
     */
    @Getter
    private int serializedSize = 0;

    /**
     * SMRRecord constructor.
     */
    public SMRRecord(String smrMethod, @NonNull Object[] smrArguments, ISerializer serializer) {
        this.SMRMethod = smrMethod;
        this.SMRArguments = smrArguments;
        this.serializerType = serializer;
    }

    /**
     * If this record is trimmed because of compaction.
     */
    public boolean isCompacted() {
        return this == COMPACTED_RECORD;
    }

    /**
     * If this byte array representing the record is
     * trimmed because of compaction.
     */
    public static boolean isCompacted(byte[] record) {
        ByteBuf buf = Unpooled.wrappedBuffer(record);
        return buf.readBoolean();
    }

    /**
     * Set the upcall result for this entry.
     */
    public void setUpcallResult(Object result) {
        upcallResult = result;
        haveUpcallResult = true;
    }

    /**
     * Set the undo record for this entry.
     */
    public void setUndoRecord(Object object) {
        this.undoRecord = object;
        undoable = true;
    }

    /**
     * Clear the undo record for this entry.
     */
    public void clearUndoRecord() {
        this.undoRecord = null;
        undoable = false;
    }

    /**
     * Deserialize from buffer and return a new {@code SMRRecord}.
     *
     * @param b byte buffer to deserialize.
     * @return an {@code SMRRecord} deserialized from buffer.
     */
    static SMRRecord deserializeFromBuffer(ByteBuf b) {
        boolean compacted = b.readBoolean();
        if (compacted) {
            return COMPACTED_RECORD;
        }
        SMRRecord record = new SMRRecord();
        record.serializedSize = b.readInt();
        short methodLength = b.readShort();
        byte[] methodBytes = new byte[methodLength];
        b.readBytes(methodBytes, 0, methodLength);
        record.SMRMethod = new String(methodBytes);
        record.serializerType = Serializers.getSerializer(b.readByte());
        byte numArguments = b.readByte();
        Object[] arguments = new Object[numArguments];
        for (byte arg = 0; arg < numArguments; arg++) {
            int len = b.readInt();
            ByteBuf objBuf = b.slice(b.readerIndex(), len);
            arguments[arg] = record.serializerType.deserialize(objBuf, null);
            b.skipBytes(len);
        }
        record.SMRArguments = arguments;
        return record;
    }

    /**
     * Serialize this record into a buffer.
     * The global address is not serialized.
     *
     * @param b byte buffer to serialize to.
     */
    void serialize(ByteBuf b) {
        int startPos = b.writerIndex();
        b.writeBoolean(isCompacted());
        if (isCompacted()) {
            // Do not write serializedSize to the buffer if compacted.
            return;
        }

        // Write total size of the record.
        int sizeIndex = b.writerIndex();
        b.writeInt(0);
        b.writeShort(SMRMethod.length());
        b.writeBytes(SMRMethod.getBytes());
        b.writeByte(serializerType.getType());
        b.writeByte(SMRArguments.length);
        Arrays.stream(SMRArguments)
                .forEach(x -> {
                    int lengthIndex = b.writerIndex();
                    b.writeInt(0);
                    serializerType.serialize(x, b);
                    int length = b.writerIndex() - lengthIndex - 4;
                    b.writerIndex(lengthIndex);
                    b.writeInt(length);
                    b.writerIndex(lengthIndex + length + 4);
                });
        // Calculate the serialized size for garbage marking purpose.
        serializedSize = b.writerIndex() - startPos;
        b.writerIndex(sizeIndex);
        b.writeInt(serializedSize);
        b.writerIndex(startPos + serializedSize);
    }

    /**
     * Get the bytes of one {@link SMRRecord} from a larger byte buffer
     * slice and transfer it to a new byte array.
     *
     * @param b byte buffer to slice from
     * @return a new byte array containing the transferred bytes
     */
    static byte[] slice(ByteBuf b) {
        int startIndex = b.readerIndex();

        boolean compacted = b.readBoolean();
        if (compacted) {
            return new byte[]{1};
        }

        int totalSize = b.readInt();
        b.readerIndex(startIndex);
        byte[] array = new byte[totalSize];
        b.readBytes(array);

        return array;
    }
}

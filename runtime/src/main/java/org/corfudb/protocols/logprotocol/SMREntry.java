package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.util.serializer.CorfuSerializer;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkState;

/**
 * Created by mwei on 1/8/16.
 */
@SuppressWarnings("checkstyle:abbreviation")
@ToString(callSuper = true)
@NoArgsConstructor
@EqualsAndHashCode
public class SMREntry extends LogEntry implements ISMRConsumable {

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

    /** An undo record, which can be used to undo this method.
     *
     */
    @Getter
    public transient Object undoRecord;

    /** A flag indicating whether an undo record is present. Necessary
     * because undo records may be NULL.
     */
    @Getter
    public boolean undoable;

    /** The upcall result, if present. */
    @Getter
    public transient Object upcallResult;

    /** If there is an upcall result for this modification. */
    @Getter
    public transient boolean haveUpcallResult = false;

    /** Set the upcall result for this entry. */
    public void setUpcallResult(Object result) {
        upcallResult = result;
        haveUpcallResult = true;
    }

    /** Set the undo record for this entry. */
    public void setUndoRecord(Object object) {
        this.undoRecord = object;
        undoable = true;
    }

    /** Clear the undo record for this entry. */
    public void clearUndoRecord() {
        this.undoRecord = null;
        undoable = false;
    }


    /** SMREntry constructor. */
    public SMREntry(String smrMethod, @NonNull Object[] smrArguments, ISerializer serializer) {
        super(LogEntryType.SMR);
        this.SMRMethod = smrMethod;
        this.SMRArguments = smrArguments;
        this.serializerType = serializer;
    }

    /**
     * This function provides the remaining buffer. Child entries
     * should initialize their contents based on the buffer.
     *
     * @param b The remaining buffer.
     */
    @Override
    void deserializeBuffer(ByteBuf b) {
        super.deserializeBuffer(b);
        short methodLength = b.readShort();
        byte[] methodBytes = new byte[methodLength];
        b.readBytes(methodBytes, 0, methodLength);
        SMRMethod = new String(methodBytes);
        serializerType = Serializers.getSerializer(b.readByte());
        byte numArguments = b.readByte();
        Object[] arguments = new Object[numArguments];
        for (byte arg = 0; arg < numArguments; arg++) {
            int len = b.readInt();
            ByteBuf objBuf = b.slice(b.readerIndex(), len);
            arguments[arg] = serializerType.deserialize(objBuf);
            b.skipBytes(len);
        }
        SMRArguments = arguments;
    }

    /**
     * Given a buffer with the reader index pointing to a serialized SMREntry, this method will
     * seek the buffer's reader index to the end of the entry.
     */
    public static void seekToEnd(ByteBuf b) {
        // Magic
        byte magicByte = b.readByte();
        checkState(magicByte == CorfuSerializer.corfuPayloadMagic, "Not a ICorfuSerializable object");
        // container type
        byte type = b.readByte();
        checkState(type == LogEntryType.SMR.asByte(), "Not a SMREntry!");
        // Method name
        short methodLength = b.readShort();
        b.skipBytes(methodLength);
        // Serializer type
        b.readByte();
        // num args
        int numArgs = b.readByte();
        for (int arg = 0; arg < numArgs; arg++) {
            int len = b.readInt();
            b.skipBytes(len);
        }
    }

    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);
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
    }

    @Override
    public List<SMREntry> getSMRUpdates(UUID id) {
        // TODO: we should check that the id matches the id of this entry,
        // but replex erases this information.
        return Collections.singletonList(this);
    }
}

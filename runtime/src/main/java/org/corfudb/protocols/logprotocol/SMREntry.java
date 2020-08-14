package org.corfudb.protocols.logprotocol;

import static com.google.common.base.Preconditions.checkState;


import io.netty.buffer.ByteBuf;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.CorfuSerializer;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

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

    @Getter
    private byte serializerId = -1;

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

    /**
     * The SMREntry's serialized size in bytes. This is set during reading from or writing to the log.
     */
    @Getter
    Integer serializedSize = null;

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
    void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        int readIndex = b.readerIndex();

        super.deserializeBuffer(b, rt);
        short methodLength = b.readShort();
        byte[] methodBytes = new byte[methodLength];
        b.readBytes(methodBytes, 0, methodLength);
        SMRMethod = new String(methodBytes);
        byte serializerId = b.readByte();
        byte numArguments = b.readByte();
        Object[] arguments = new Object[numArguments];

        if (!opaque) {
            serializerType = Serializers.getSerializer(serializerId);
        } else {
            this.serializerId = serializerId;
        }

        for (byte arg = 0; arg < numArguments; arg++) {
            int len = b.readInt();
            ByteBuf objBuf = b.slice(b.readerIndex(), len);
            if (opaque) {
                byte[] argBytes = new byte[len];
                objBuf.readBytes(argBytes);
                arguments[arg] = argBytes;
            } else {
                arguments[arg] = serializerType.deserialize(objBuf, rt);
            }
            b.skipBytes(len);
        }
        SMRArguments = arguments;
        serializedSize = b.readerIndex() - readIndex + 1;
    }

    /**
     * Given a buffer with the logreader index pointing to a serialized SMREntry, this method will
     * seek the buffer's logreader index to the end of the entry.
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
        int startWriterIndex = b.writerIndex();
        super.serialize(b);
        b.writeShort(SMRMethod.length());
        b.writeBytes(SMRMethod.getBytes());
        if (opaque) {
            //TODO(Maithem) add test for serialize/desrialize of opaque entries
            if (serializerId == -1) {
                throw new IllegalStateException("opaque entry doesn't have a serializer id");
            }
            b.writeByte(serializerId);
        } else {
            b.writeByte(serializerType.getType());
        }
        b.writeByte(SMRArguments.length);
        Arrays.stream(SMRArguments)
                .forEach(x -> {
                    int lengthIndex = b.writerIndex();
                    b.writeInt(0);
                    if (opaque) {
                        b.writeBytes((byte[]) x);
                    } else {
                        serializerType.serialize(x, b);
                    }
                    int length = b.writerIndex() - lengthIndex - 4;
                    b.writerIndex(lengthIndex);
                    b.writeInt(length);
                    b.writerIndex(lengthIndex + length + 4);
                });
        serializedSize = b.writerIndex() - startWriterIndex;
    }

    @Override
    public List<SMREntry> getSMRUpdates(UUID id) {
        // TODO: we should check that the id matches the id of this entry,
        // but replex erases this information.
        return Collections.singletonList(this);
    }
}

package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang.CharSet;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Created by mwei on 1/8/16.
 */
@SuppressWarnings("checkstyle:abbreviation")
@ToString(callSuper = true)
@NoArgsConstructor
public class SMRRecord {

    private static Charset DEFAULT_CHAR_SET = Charset.forName("UTF-8");
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
     * The global address this record is associated to.
     */
    @Getter
    @Setter
    long globalAddress = Address.NON_ADDRESS;

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
     * SMRRecord constructor.
     */
    public SMRRecord(String smrMethod, @NonNull Object[] smrArguments, ISerializer serializer) {
        this.SMRMethod = smrMethod;
        this.SMRArguments = smrArguments;
        this.serializerType = serializer;
    }

    /**
     * Deserialize from buffer and return a new {@code SMRRecord}.
     *
     * @param b byte buffer to deserialize.
     * @return an {@code SMRRecord} deserialized from buffer.
     */
    static SMRRecord deserializeFromBuffer(ByteBuf b, CorfuRuntime rt) {
        SMRRecord record = new SMRRecord();
        short methodLength = b.readShort();
        byte[] methodBytes = new byte[methodLength];
        b.readBytes(methodBytes, 0, methodLength);
        record.SMRMethod = new String(methodBytes, DEFAULT_CHAR_SET);
        record.serializerType = Serializers.getSerializer(b.readByte());
        byte numArguments = b.readByte();
        Object[] arguments = new Object[numArguments];
        for (byte arg = 0; arg < numArguments; arg++) {
            int len = b.readInt();
            ByteBuf objBuf = b.slice(b.readerIndex(), len);
            arguments[arg] = record.serializerType.deserialize(objBuf, rt);
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
        b.writeShort(SMRMethod.length());
        b.writeBytes(SMRMethod.getBytes(DEFAULT_CHAR_SET));
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
}

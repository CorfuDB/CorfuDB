package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.SMREntryWithLocator;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

/**
 * Created by mwei on 1/8/16.
 */
@Deprecated // TODO: Add replacement method that conforms to style
@SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
@ToString(callSuper = true)
@NoArgsConstructor
public class SMREntry extends LogEntry implements ISMRWithLocatorConsumable {

    public static class SMREntryLocator implements ISMREntryLocator {
        /**
         * The globalAddress that the SMREntry is from in the global log.
         */
        @Getter
        private final long globalAddress;

        public SMREntryLocator(long globalAddress) {
            this.globalAddress = globalAddress;
        }

        @Override
        public int compareTo(ISMREntryLocator other) {
            long otherAddress = other.getGlobalAddress();
            if (otherAddress == globalAddress) {
                if (other instanceof  SMREntryLocator) {
                    return 0;
                } else {
                    throw new RuntimeException("SMREntries of the same global address have different SMREntry type");
                }
            } else {
                return Long.compare(globalAddress, otherAddress);
            }
        }
    }

    /**
     * The name of the SMR method. Note that this is limited to the size of a short.
     */
    @Deprecated // TODO: Add replacement method that conforms to style
    @SuppressWarnings("checkstyle:MemberName") // Due to deprecation
    @Getter
    protected String SMRMethod;

    /**
     * The arguments to the SMR method, which could be 0.
     */
    @Deprecated // TODO: Add replacement method that conforms to style
    @SuppressWarnings("checkstyle:MemberName") // Due to deprecation
    @Getter
    protected Object[] SMRArguments;

    /**
     * The serializer used to serialize the SMR arguments.
     */
    @Getter
    protected ISerializer serializerType;

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

    public SMREntry(@NonNull SMREntry smrEntry) {
        super(smrEntry);
        this.SMRMethod = smrEntry.getSMRMethod();
        this.SMRArguments = smrEntry.getSMRArguments();
        this.serializerType = smrEntry.getSerializerType();
        this.undoRecord = smrEntry.undoRecord;
        this.undoable = smrEntry.undoable;
        this.upcallResult = smrEntry.upcallResult;
        this.haveUpcallResult = smrEntry.haveUpcallResult;
    }

    /**
     * This function provides the remaining buffer. Child entries
     * should initialize their contents based on the buffer.
     *
     * @param b The remaining buffer.
     */
    @Override
    void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        super.deserializeBuffer(b, rt);
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
            arguments[arg] = serializerType.deserialize(objBuf, rt);
            b.skipBytes(len);
        }
        SMRArguments = arguments;
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
        return Collections.singletonList(this);
    }

    @Override
    public List<SMREntryWithLocator> getSMRWithLocatorUpdates(long globalAddress, UUID id) {
        // TODO: we should check that the id matches the id of this entry,
        // but replex erases this information.
        SMREntryLocator locator = new SMREntryLocator(globalAddress);
        SMREntryWithLocator smrEntryWithLocator = new SMREntryWithLocator(this, locator);
        return Collections.singletonList(smrEntryWithLocator);
    }
}

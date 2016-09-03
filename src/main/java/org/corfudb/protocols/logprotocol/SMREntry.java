package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.SerializerType;
import org.corfudb.util.serializer.Serializers;

import java.util.Arrays;

/**
 * Created by mwei on 1/8/16.
 */
@ToString(callSuper = true)
@NoArgsConstructor
public class SMREntry extends LogEntry {

    /**
     * The name of the SMR method. Note that this is limited to the size of a short.
     */
    @Getter
    private String SMRMethod;

    /**
     * The arguments to the SMR method, which could be 0.
     */
    @Getter
    private Object[] SMRArguments;

    /**
     * The serializer used to serialize the SMR arguments.
     */
    @Getter
    private SerializerType serializerType;

    public SMREntry(String SMRMethod, @NonNull Object[] SMRArguments, SerializerType serializer) {
        super(LogEntryType.SMR);
        this.SMRMethod = SMRMethod;
        this.SMRArguments = SMRArguments;
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
        super.deserializeBuffer(b, rt);
        short methodLength = b.readShort();
        byte[] methodBytes = new byte[methodLength];
        b.readBytes(methodBytes, 0, methodLength);
        SMRMethod = new String(methodBytes);
        serializerType = Serializers.typeMap.get(b.readByte());
        byte numArguments = b.readByte();
        Object[] arguments = new Object[numArguments];
        for (byte arg = 0; arg < numArguments; arg++) {
            int len = b.readInt();
            ByteBuf objBuf = b.slice(b.readerIndex(), len);
            arguments[arg] = Serializers.getSerializer(serializerType).deserialize(objBuf, rt);
            b.skipBytes(len);
        }
        SMRArguments = arguments;
    }

    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeShort(SMRMethod.length());
        b.writeBytes(SMRMethod.getBytes());
        b.writeByte(serializerType.asByte());
        b.writeByte(SMRArguments.length);
        Arrays.stream(SMRArguments)
                .forEach(x -> {
                    int lengthIndex = b.writerIndex();
                    b.writeInt(0);
                    Serializers.getSerializer(serializerType).serialize(x, b);
                    int length = b.writerIndex() - lengthIndex - 4;
                    b.writerIndex(lengthIndex);
                    b.writeInt(length);
                    b.writerIndex(lengthIndex + length + 4);
                });
    }
}

package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import org.corfudb.util.serializer.ICorfuSerializable;
import org.corfudb.util.serializer.Serializers;

/**
 * Created by mwei on 1/8/16.
 */
public class SMREntry extends LogEntry {

    /**
     *  The name of the SMR method. Note that this is limited to the size of a short.
     */
    @Getter
    private String SMRMethod;

    @Getter
    private Object[] SMRArguments;

    @Getter
    private Serializers.SerializerType serializerType;

    public SMREntry(String SMRMethod, Object[] SMRArguments, Serializers.SerializerType serializer)
    {
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
    void deserializeBuffer(ByteBuf b) {
        super.deserializeBuffer(b);
        short methodLength = b.readShort();
        byte[] methodBytes = new byte[methodLength];
        b.readBytes(methodBytes, 0, methodLength);
        SMRMethod = new String(methodBytes);

    }

    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeShort(SMRMethod.length());
        b.writeBytes(SMRMethod.getBytes());
    }
}

package org.corfudb.runtime.smr.smrprotocol;

import io.netty.buffer.ByteBuf;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.smr.ISMREngine;
import org.corfudb.util.serializer.ICorfuSerializable;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by mwei on 9/29/15.
 */
@Slf4j
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SMRCommand<T,R> implements ICorfuSerializable {
    //region Command Type
    @RequiredArgsConstructor
    public enum SMRCommandType {
        // Type of SMR command
        NOP(0, SMRCommand.class),
        LAMBDA_COMMAND(20, LambdaSMRCommand.class),
        METHOD_TOKEN(21, MethodTokenSMRCommand.class)
        ;

        final int type;
        final Class<? extends SMRCommand> messageType;

        byte asByte() { return (byte)type; }
    };

    static Map<Byte, SMRCommandType> typeMap =
            Arrays.<SMRCommandType>stream(SMRCommandType.values())
                    .collect(Collectors.toMap(SMRCommandType::asByte, Function.identity()));
    //endregion

    //region Fields
    /* The type of SMR command being executed. */
    SMRCommandType type;
    //endregion

    //region Methods
    /* The operation to execute when the command is processed by the SMR engine. */
    public R execute(T state, ISMREngine<T> engine) {return null;};
    //endregion

    //region Serialization
    /** Parse the rest of the message from the buffer. Classes that extend SMRCommand
     * should parse their fields in this method.
     * @param buffer
     */
    public void fromBuffer(ByteBuf buffer) {
        // we don't do anything here since we've already read the necessary fields.
    }

    /** Serialize the message into the given bytebuffer.
     * @param buffer    The buffer to serialize to.
     * */
    public void serialize(ByteBuf buffer) {
        buffer.writeByte(type.asByte());
    }

    /** Take the given bytebuffer and deserialize it into a message.
     *
     * @param buffer    The buffer to deserialize.
     * @return          The corresponding message.
     */
    @SneakyThrows
    public static SMRCommand deserialize(ByteBuf buffer) {
        SMRCommandType t = typeMap.get(buffer.readByte());
        SMRCommand cmd = t.messageType.getConstructor().newInstance();
        cmd.setType(t);
        cmd.fromBuffer(buffer);
        return cmd;
    }
    //endregion
}

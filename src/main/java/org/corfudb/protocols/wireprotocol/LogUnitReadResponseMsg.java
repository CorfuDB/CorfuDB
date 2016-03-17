package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.*;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.runtime.CorfuRuntime;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Created by mwei on 9/15/15.
 */
@Getter
@Setter
@NoArgsConstructor
@ToString
public class LogUnitReadResponseMsg extends LogUnitPayloadMsg {


    @RequiredArgsConstructor
    public enum ReadResultType {
        EMPTY(0),
        DATA(1),
        FILLED_HOLE(2),
        TRIMMED(3)
        ;

        final int type;

        public byte asByte() { return (byte)type; }
    }

    public static Map<Byte, ReadResultType> readResultTypeMap =
            Arrays.<ReadResultType>stream(ReadResultType.values())
                    .collect(Collectors.toMap(ReadResultType::asByte, Function.identity()));

    @Data
    @RequiredArgsConstructor
    @AllArgsConstructor
    public static class LogUnitEntry implements IMetadata {
        public final ByteBuf buffer;
        public final EnumMap<IMetadata.LogUnitMetadataType, Object> metadataMap;
        public final boolean isHole;
        public boolean isPersisted;

        /** Generate a new log unit entry which is a hole */
        public LogUnitEntry()
        {
            buffer = null;
            metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);
            isHole = true;
            isPersisted = false;
        }
    }

    public static class ReadResult implements ILogUnitEntry {

        /** The backing message for this read result. */
        LogUnitReadResponseMsg msg;

        public ReadResult(LogUnitReadResponseMsg msg)
        {
            this.msg = msg;
        }

        @Getter(lazy=true)
        private final ReadResultType resultType = msg.getResult();

        @Getter(lazy=true)
        private final EnumMap<IMetadata.LogUnitMetadataType, Object> metadataMap = msg.getMetadataMap();

        @Getter(lazy=true)
        private final ByteBuf buffer = msg.getData();

        public Object getPayload(CorfuRuntime rt) {
            Object o = msg.getPayload(rt);
            if (o instanceof LogEntry)
            {
                ((LogEntry) o).setEntry(this);
            }
            return o;
        }

        @Getter(lazy=true)
        private final int sizeEstimate = calculateSize();

        private int calculateSize() {
            if (msg.getResult() != ReadResultType.DATA) {
                return 1; // Non-data objects essentially take up no space.
            }
            else {
                // If we have a payload, get that.
                return msg.getData().readableBytes();
            }
        }
    }

    /** The result of this read. */
    ReadResultType result;

    public LogUnitReadResponseMsg(ReadResultType result)
    {
        this.msgType = CorfuMsgType.READ_RESPONSE;
        this.result = result;
    }

    public LogUnitReadResponseMsg(LogUnitEntry entry)
    {
        this.msgType = CorfuMsgType.READ_RESPONSE;
        this.result = ReadResultType.DATA;
        this.setMetadataMap(entry.getMetadataMap());
        this.setData(entry.getBuffer());
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeByte(result.asByte());
    }

    /**
     * Parse the rest of the message from the buffer. Classes that extend CorfuMsg
     * should parse their fields in this method.
     *
     * @param buffer
     */
    @Override
    public void fromBuffer(ByteBuf buffer) {
        super.fromBuffer(buffer);
        result = readResultTypeMap.get(buffer.readByte());
    }
}

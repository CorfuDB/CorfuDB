package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.corfudb.util.serializer.ICorfuSerializable;
import sun.rmi.runtime.Log;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by mwei on 1/8/16.
 */
@ToString
@NoArgsConstructor
public class LogEntry implements ICorfuSerializable {

    @RequiredArgsConstructor
    public enum LogEntryType {
        // Base Messages
        NOP(0, LogEntry.class),
        SMR(1, SMREntry.class)
        ;

        public final int type;
        public final Class<? extends LogEntry> entryType;

        public byte asByte() { return (byte)type; }
    };

    static final Map<Byte, LogEntryType> typeMap =
            Arrays.stream(LogEntryType.values())
                    .collect(Collectors.toMap(LogEntryType::asByte, Function.identity()));

    /** The type of log entry */
    @Getter
    LogEntryType type;

    /** Constructor for generating LogEntries.
     *
     * @param type  The type of log entry to instantiate.
     */
    public LogEntry(LogEntryType type)
    {
        this.type = type;
    }

    /** This function provides the remaining buffer. Child entries
     * should initialize their contents based on the buffer.
     * @param b The remaining buffer.
     */
    void deserializeBuffer(ByteBuf b) {
        // In the base case, we don't do anything.
    }

    /**
     * The base LogEntry format is very simple. The first byte represents the type
     * of entry, and the rest of the format is dependent on the the entry type.
     * @param b The buffer to deserialize.
     * @return  A LogEntry.
     */
    public static ICorfuSerializable deserialize(ByteBuf b) {
        try {
            LogEntryType let = typeMap.get(b.readByte());
            LogEntry l = let.entryType.newInstance();
            l.type = let;
            l.deserializeBuffer(b);
            return l;
        } catch (InstantiationException | IllegalAccessException ie)
        {
            throw new RuntimeException("Error deserializing entry", ie);
        }
    }

    /**
     * Serialize the given LogEntry into a given byte buffer.
     * @param b The buffer to serialize into.
     */
    @Override
    public void serialize(ByteBuf b) {
        b.writeByte(type.asByte());
    }

}

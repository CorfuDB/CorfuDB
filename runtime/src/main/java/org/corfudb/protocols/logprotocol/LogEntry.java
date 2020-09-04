package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.serializer.ICorfuSerializable;


/**
 * Created by mwei on 1/8/16.
 */
@ToString(exclude = {"runtime"})
@NoArgsConstructor
public class LogEntry implements ICorfuSerializable {

    static final Map<Byte, LogEntryType> typeMap =
            Arrays.stream(LogEntryType.values())
                    .collect(Collectors.toMap(LogEntryType::asByte, Function.identity()));

    /**
     * The runtime to use.
     */
    @Setter
    protected CorfuRuntime runtime;

    /**
     * The type of log entry.
     */
    @Getter
    LogEntryType type;

    /**
     * The global address of this log entry.
     */
    @Getter
    @Setter
    long globalAddress = Address.NON_ADDRESS;

    @Getter
    @Setter
    boolean opaque = false;

    /**
     * Constructor for generating LogEntries.
     *
     * @param type The type of log entry to instantiate.
     */
    public LogEntry(LogEntryType type) {
        this.type = type;
    }

    /**
     * The base LogEntry format is very simple. The first byte represents the type
     * of entry, and the rest of the format is dependent on the the entry type.
     *
     * @param b The buffer to deserialize.
     * @return A LogEntry.
     */
    public static ICorfuSerializable deserialize(ByteBuf b, CorfuRuntime rt) {
        return deserialize(b, rt, false);
    }

    public static ICorfuSerializable deserialize(ByteBuf b, CorfuRuntime rt, boolean opaque) {
        try {
            byte type = b.readByte();
            LogEntryType let = typeMap.get(type);
            LogEntry l = let.entryType.newInstance();
            l.type = let;
            l.runtime = rt;
            l.opaque = opaque;
            l.deserializeBuffer(b, rt);
            return l;
        } catch (InstantiationException | IllegalAccessException ie) {
            throw new RuntimeException("Error deserializing entry", ie);
        }
    }

    /**
     * This function provides the remaining buffer. Child entries
     * should initialize their contents based on the buffer.
     *
     * @param b The remaining buffer.
     */
    void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        // In the base case, we don't do anything.
    }

    /**
     * Serialize the given LogEntry into a given byte buffer.
     *
     * @param b The buffer to serialize into.
     */
    @Override
    public void serialize(ByteBuf b) {
        b.writeByte(type.asByte());
    }

    @RequiredArgsConstructor
    public enum LogEntryType {
        // Base Messages
        NOP(0, LogEntry.class),
        SMR(1, SMREntry.class),
        MULTIOBJSMR(7, MultiObjectSMREntry.class),
        MULTISMR(8, MultiSMREntry.class),
        CHECKPOINT(10, CheckpointEntry.class);

        public final int type;
        public final Class<? extends LogEntry> entryType;

        public byte asByte() {
            return (byte) type;
        }
    }
}

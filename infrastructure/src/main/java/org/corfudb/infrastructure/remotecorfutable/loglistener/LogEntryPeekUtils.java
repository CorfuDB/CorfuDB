package org.corfudb.infrastructure.remotecorfutable.loglistener;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import org.corfudb.protocols.logprotocol.LogEntry;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class LogEntryPeekUtils {
    private static final Map<Byte, LogEntry.LogEntryType> typeMap =
            ImmutableMap.copyOf(Arrays.stream(LogEntry.LogEntryType.values())
                    .collect(Collectors.toMap(LogEntry.LogEntryType::asByte, Function.identity())));

    public static LogEntry.LogEntryType getType(ByteBuf buffer, int startPoint) {
        return typeMap.get(buffer.getByte(startPoint));
    }

    public static String getSMRName(ByteBuf buffer, int startPoint) {
        short methodLength = buffer.getShort(startPoint);
        byte[] methodBytes = new byte[methodLength];
        buffer.getBytes(startPoint + Short.BYTES, methodBytes, 0, methodLength);
        return new String(methodBytes);
    }
}

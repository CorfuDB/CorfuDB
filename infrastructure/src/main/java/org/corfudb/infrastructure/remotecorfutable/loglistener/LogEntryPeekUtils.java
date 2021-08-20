package org.corfudb.infrastructure.remotecorfutable.loglistener;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import org.corfudb.protocols.logprotocol.LogEntry;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class contains utilities to help extract SMR methods and arguments from LogData without deserialization.
 * This is used to help place Remote Corfu Table entries from the log into the database.
 *
 * Created by nvaishampayan517 on 08/19/21
 */
public final class LogEntryPeekUtils {

    //prevent instantiation
    private LogEntryPeekUtils() {}

    private static final Map<Byte, LogEntry.LogEntryType> typeMap =
            ImmutableMap.copyOf(Arrays.stream(LogEntry.LogEntryType.values())
                    .collect(Collectors.toMap(LogEntry.LogEntryType::asByte, Function.identity())));

    public static LogEntry.LogEntryType getType(ByteBuffer buffer, int startPoint) {
        return typeMap.get(buffer.get(startPoint));
    }

    public static byte[] getSMRNameFromSMREntry(ByteBuffer buffer, int startPoint) {
        short methodLength = buffer.getShort(startPoint);
        byte[] methodBytes = new byte[methodLength];
        buffer.position(startPoint + Short.BYTES);
        buffer.get(methodBytes, 0, methodLength);
        buffer.rewind();
        return methodBytes;
    }

    public static ByteString[] getArgsFromSMREntry(ByteBuffer buffer, int startPoint) {
        buffer.position(startPoint);
        byte numArgs = buffer.get();
        ByteString[] argsList = new ByteString[numArgs];
        for (byte i = 0; i < numArgs; i++) {
            int len = buffer.getInt();
            argsList[i] = ByteString.copyFrom(buffer, len);
        }
        buffer.rewind();
        return argsList;
    }
}

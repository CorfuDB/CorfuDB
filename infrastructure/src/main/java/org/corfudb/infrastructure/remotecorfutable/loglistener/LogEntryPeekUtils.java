package org.corfudb.infrastructure.remotecorfutable.loglistener;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import org.corfudb.protocols.logprotocol.LogEntry;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

    /**
     * Reads in the type bit from the buffer, then converts it to a LogEntryType.
     * This method will move the position of the buffer.
     * @param buffer The buffer containing the type bit. If position is not placed at the type bit of the LogData,
     *               undefined behavior.
     * @return The Type of the LogEntry.
     */
    public static LogEntry.LogEntryType getType(ByteBuffer buffer) {
        return typeMap.get(buffer.get());
    }

    /**
     * Reads SMR method name from the given buffer.
     * This method will move the position of the buffer.
     * @param buffer The buffer containing the SMR method name. If position is not set to the start of the method name
     *               section, undefined behavior.
     * @return The byte[] representing the encoded SMR method name.
     */
    public static byte[] getSMRNameFromSMREntry(ByteBuffer buffer) {
        short methodLength = buffer.getShort();
        byte[] methodBytes = new byte[methodLength];
        buffer.get(methodBytes, 0, methodLength);
        return methodBytes;
    }

    /**
     * Reads SMRentry arguments from the given buffer.
     * This method will move the position of the buffer.
     * @param buffer The buffer containing the SMR arguments. If position is not set to the start of the arguments
     *               section, undefined behavior.
     * @return The ByteString list containing the encoded arguments
     */
    public static List<ByteString> getArgsFromSMREntry(ByteBuffer buffer) {
        byte numArgs = buffer.get();
        List<ByteString> argsList = new ArrayList<>(numArgs);
        for (byte i = 0; i < numArgs; i++) {
            int len = buffer.getInt();
            argsList.add(ByteString.copyFrom(buffer, len));
        }
        return Collections.unmodifiableList(argsList);
    }
}

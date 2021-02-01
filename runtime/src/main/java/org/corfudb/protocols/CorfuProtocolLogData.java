package org.corfudb.protocols;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.proto.LogData.LogDataMsg;
import org.corfudb.runtime.proto.LogData.ReadResponseMsg;

/**
 * This class provides methods for creating and converting between the Protobuf
 * objects defined in log_data.proto and their Java counterparts. Used by the
 * LogUnit RPCs.
 */
@Slf4j
public final class CorfuProtocolLogData {
    // Prevent class from being instantiated
    private CorfuProtocolLogData() {}

    /**
     * Returns the Protobuf representation of a LogData object.
     *
     * @param logData  the desired LogData object
     * @return         an equivalent Protobuf LogData message
     */
    public static LogDataMsg getLogDataMsg(LogData logData) {
        ByteBuf buf = Unpooled.buffer();
        logData.doSerialize(buf);

        return LogDataMsg.newBuilder()
                .setEntry(ByteString.copyFrom(buf.resetReaderIndex().array()))
                .build();
    }

    /**
     * Returns a LogData object from its Protobuf representation.
     *
     * @param msg  the desired Protobuf LogData message
     * @return     a equivalent LogData object
     */
    public static LogData getLogData(LogDataMsg msg) {
        return new LogData(Unpooled.wrappedBuffer(msg.getEntry().asReadOnlyByteBuffer()));
    }

    /**
     * The result of a single READ from the specified address.
     *
     * @param address  the read address
     * @param logData  the data read from the specified address
     * @return         a ReadResponseMsg containing the data read
     */
    public static ReadResponseMsg getReadResponseMsg(long address, LogData logData) {
        return ReadResponseMsg.newBuilder()
                .setAddress(address)
                .setLogData(getLogDataMsg(logData))
                .build();
    }
}

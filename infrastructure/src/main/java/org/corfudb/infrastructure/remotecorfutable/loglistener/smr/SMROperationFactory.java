package org.corfudb.infrastructure.remotecorfutable.loglistener.smr;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.infrastructure.remotecorfutable.loglistener.LogEntryPeekUtils;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;

import java.util.UUID;

/**
 * This class assists in creation of SMROperations.
 *
 * Created by nvaishampayan517 on 08/19/21
 */
public final class SMROperationFactory {
    //prevent instantiation of factory
    private SMROperationFactory() {}

    public static SMROperation getSMROperation(LogData data, UUID streamId) {
        if (data.getType() == DataType.DATA) {
            ByteBuf dataBuffer = Unpooled.wrappedBuffer(data.getData());
            switch (LogEntryPeekUtils.getType(dataBuffer, 0)) {
                case SMR:
                    //switch over the method types
                    return null;
                case NOP:
                case MULTISMR:
                case CHECKPOINT:
                case MULTIOBJSMR:
                    throw new UnsupportedOperationException("These types will be supported in the future.");
                default:
                    throw new IllegalArgumentException("Unknown Log Entry Type");
            }
        } else {
            throw new IllegalArgumentException("SMR operation can only be created from DATA type");
        }
    }
}

package org.corfudb.infrastructure.remotecorfutable.loglistener.smr;

import com.google.protobuf.ByteString;
import org.corfudb.infrastructure.remotecorfutable.loglistener.LogEntryPeekUtils;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTableSMRMethods;

import java.nio.ByteBuffer;
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
        int startPoint = 0;
        if (data.getType() == DataType.DATA) {
            ByteBuffer dataBuffer = ByteBuffer.wrap(data.getData());
            long timestamp = data.getGlobalAddress();
            LogEntry.LogEntryType dataType = LogEntryPeekUtils.getType(dataBuffer, startPoint);
            startPoint+=1;
            ByteString[] smrArgs;
            switch (dataType) {
                case SMR:
                    byte[] methodBytes = LogEntryPeekUtils.getSMRNameFromSMREntry(dataBuffer,startPoint);
                    RemoteCorfuTableSMRMethods methodType =
                        RemoteCorfuTableSMRMethods.getMethodFromName(new String(methodBytes));
                    switch (methodType) {
                        case CLEAR:
                            return new ClearOperation(timestamp, streamId);
                        case DELETE:
                            //moving past method name and length
                            startPoint+=methodBytes.length + Short.BYTES;
                            //skipping the serializer ID
                            startPoint+=1;
                            smrArgs = LogEntryPeekUtils.getArgsFromSMREntry(dataBuffer, startPoint);
                            return new DeleteOperation(smrArgs, timestamp, streamId);
                        case UPDATE:
                            //moving past method name and length
                            startPoint+=methodBytes.length + Short.BYTES;
                            //skipping the serializer ID
                            startPoint+=1;
                            smrArgs = LogEntryPeekUtils.getArgsFromSMREntry(dataBuffer, startPoint);
                            return new UpdateOperation(smrArgs, timestamp, streamId);
                        default:
                            throw new IllegalArgumentException("Unknown method for RemoteCorfuTable");
                    }
                    //switch over the method types
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

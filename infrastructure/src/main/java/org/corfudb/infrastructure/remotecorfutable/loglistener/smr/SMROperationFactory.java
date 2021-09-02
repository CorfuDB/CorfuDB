package org.corfudb.infrastructure.remotecorfutable.loglistener.smr;

import com.google.protobuf.ByteString;
import org.corfudb.infrastructure.remotecorfutable.loglistener.LogEntryPeekUtils;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTableSMRMethods;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
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
            ByteBuffer dataBuffer = ByteBuffer.wrap(data.getData());
            long timestamp = data.getGlobalAddress();
            LogEntry.LogEntryType dataType = LogEntryPeekUtils.getType(dataBuffer);

            switch (dataType) {
                case SMR:
                    return handleSMREntry(streamId, dataBuffer, timestamp);
                case MULTISMR:
                    return handleMultiSMREntry(streamId, dataBuffer, timestamp);
                case NOP:
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

    private static SMROperation handleMultiSMREntry(UUID streamId, ByteBuffer dataBuffer, long timestamp) {
        List<SMROperation> subOperations = new LinkedList<>();
        int numSubOps = dataBuffer.getInt();
        for (int i = 0; i < numSubOps; i++) {
            //remove magic byte
            dataBuffer.get();
            //verify entry type is SMR
            LogEntry.LogEntryType dataType = LogEntryPeekUtils.getType(dataBuffer);
            if (dataType != LogEntry.LogEntryType.SMR) {
                throw new IllegalArgumentException("MultiSMR should only contain SMR Entries");
            }
            SMROperation subOp = handleSMREntry(streamId, dataBuffer, timestamp);
            subOperations.add(subOp);
        }

        return new CompositeOperation(subOperations, streamId, timestamp);
    }

    private static SMROperation handleSMREntry(UUID streamId, ByteBuffer dataBuffer, long timestamp) {
        byte[] methodBytes = LogEntryPeekUtils.getSMRNameFromSMREntry(dataBuffer);
        RemoteCorfuTableSMRMethods methodType =
            RemoteCorfuTableSMRMethods.getMethodFromName(new String(methodBytes));
        //consume serializer ID
        dataBuffer.get();
        //required even in CLEAR case to consume the numArgs value
        List<ByteString> smrArgs = LogEntryPeekUtils.getArgsFromSMREntry(dataBuffer);
        switch (methodType) {
            case CLEAR:
                return new ClearOperation(timestamp, streamId);
            case DELETE:
                return new DeleteOperation(smrArgs, timestamp, streamId);
            case UPDATE:
                return new UpdateOperation(smrArgs, timestamp, streamId);
            default:
                throw new IllegalArgumentException(String.format("Improper Method stored for RemoteCorfuTable: %s",
                        methodType.getSMRName()));
        }
    }
}

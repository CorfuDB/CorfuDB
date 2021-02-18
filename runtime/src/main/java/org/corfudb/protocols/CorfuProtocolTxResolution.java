package org.corfudb.protocols;

import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.proto.TxResolution.TxResolutionInfoMsg;
import org.corfudb.runtime.proto.TxResolution.UuidToListOfBytesPairMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.corfudb.protocols.CorfuProtocolCommon.getTokenMsg;
import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;

/**
 * This class provides methods for creating and converting between the Protobuf
 * objects defined in tx_resolution.proto and their Java counterparts. These are used
 * by the several RPCs with Sequencer ones being the main one.
 */
@Slf4j
public final class CorfuProtocolTxResolution {
    /**
     * Returns the Java conflicts Map of UUID to a set of bytes array from the list of Protobuf
     * {@link UuidToListOfBytesPairMsg} objects.
     *
     * @param conflicts the list of Protobuf {@link UuidToListOfBytesPairMsg} objects
     * @return the Java conflicts Map of UUID to a set of bytes array
     */
    private static Map<UUID, Set<byte[]>> getConflictsMap(List<UuidToListOfBytesPairMsg> conflicts) {
        return conflicts.stream()
                .collect(Collectors.<UuidToListOfBytesPairMsg, UUID, Set<byte[]>>toMap(
                        entry -> getUUID(entry.getKey()),
                        entry -> entry.getValueList()
                                .stream()
                                .map(ByteString::toByteArray)
                                .collect(Collectors.toSet())
                ));
    }

    /**
     * Returns the Protobuf {@link TxResolutionInfoMsg} object from the Java {@link TxResolutionInfo} object.
     *
     * @param txResolutionInfo the Java {@link TxResolutionInfo} object
     * @return the Protobuf {@link TxResolutionInfoMsg} object
     */
    public static TxResolutionInfoMsg getTxResolutionInfoMsg(TxResolutionInfo txResolutionInfo) {
        TxResolutionInfoMsg.Builder txResolutionInfoBuilder = TxResolutionInfoMsg.newBuilder();

        txResolutionInfo.getConflictSet().forEach((uuid, bytes) -> {
            // Create a List of ByteStrings for each UUID
            // ByteString is the Protobuf's representation of Immutable sequence of bytes (similar to byte[])
            List<ByteString> byteStringList = new ArrayList<>();

            // Parse the Set of array of bytes(byte[]) for a UUID and
            // create a ByteString for each entry(byte[]) and add to byteStringList
            bytes.forEach(b -> byteStringList.add(ByteString.copyFrom(b)));

            // Add the newly created entry of UuidToListOfBytesPairMsg
            txResolutionInfoBuilder.addConflictSet(
                    UuidToListOfBytesPairMsg.newBuilder()
                            .setKey(getUuidMsg(uuid))
                            .addAllValue(byteStringList)
                            .build()
            );
        });

        txResolutionInfo.getWriteConflictParams().forEach((uuid, bytes) -> {
            // Create a List of ByteStrings for each UUID
            // ByteString is the Protobuf's representation of Immutable sequence of bytes (similar to byte[])
            List<ByteString> byteStringList = new ArrayList<>();

            // Parse the Set of array of bytes(byte[]) for each UUID and
            // create a ByteString for each entry(byte[]) and add to byteStringList
            bytes.forEach(b -> byteStringList.add(ByteString.copyFrom(b)));

            // Add the newly created entry of UuidToListOfBytesPairMsg
            txResolutionInfoBuilder.addWriteConflictParamsSet(
                    UuidToListOfBytesPairMsg.newBuilder()
                            .setKey(getUuidMsg(uuid))
                            .addAllValue(byteStringList)
                            .build()
            );
        });

        return txResolutionInfoBuilder.setTxId(getUuidMsg(txResolutionInfo.getTXid()))
                .setSnapshotTimestamp(getTokenMsg(txResolutionInfo.getSnapshotTimestamp()))
                .build();
    }

    /**
     * Returns the Java {@link TxResolutionInfo} object from the Protobuf {@link TxResolutionInfoMsg} object.
     *
     * @param msg the Protobuf {@link TxResolutionInfoMsg} object
     * @return the Java {@link TxResolutionInfo} object
     */
    public static TxResolutionInfo getTxResolutionInfo(TxResolutionInfoMsg msg) {
        return new TxResolutionInfo(
                getUUID(msg.getTxId()),
                Token.of(msg.getSnapshotTimestamp().getEpoch(),
                        msg.getSnapshotTimestamp().getSequence()),
                getConflictsMap(msg.getConflictSetList()),
                getConflictsMap(msg.getWriteConflictParamsSetList())
        );
    }
}

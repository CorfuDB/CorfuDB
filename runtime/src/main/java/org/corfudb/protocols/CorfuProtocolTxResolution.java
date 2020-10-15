package org.corfudb.protocols;

import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.proto.TxResolution.TxResolutionInfoMsg;
import org.corfudb.runtime.proto.TxResolution.UuidToListOfBytesPairMsg;

import java.util.*;
import java.util.stream.Collectors;

import static org.corfudb.protocols.CorfuProtocolCommon.*;

@Slf4j
public class CorfuProtocolTxResolution {
    public static TxResolutionInfoMsg getTxResolutionInfoMsg(TxResolutionInfo txResolutionInfo) {
        TxResolutionInfoMsg.Builder txResolutionInfoBuilder = TxResolutionInfoMsg.newBuilder();

        txResolutionInfo.getConflictSet().forEach((uuid, bytes) -> {
            // Create a List of ByteStrings for each UUID
            // ByteString = Protobuf's Immutable sequence of bytes (similar to byte[])
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
            // ByteString = Protobuf's Immutable sequence of bytes (similar to byte[])
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

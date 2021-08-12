package org.corfudb.protocols;

import com.google.protobuf.ByteString;
import lombok.NonNull;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.index.qual.Positive;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableEntry;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableVersionedKey;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import org.corfudb.protocols.wireprotocol.remotecorfutable.ContainsResponse;
import org.corfudb.protocols.wireprotocol.remotecorfutable.GetResponse;
import org.corfudb.protocols.wireprotocol.remotecorfutable.ScanResponse;
import org.corfudb.protocols.wireprotocol.remotecorfutable.SizeResponse;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.RemoteCorfuTable.RemoteCorfuTableEntryMsg;
import org.corfudb.runtime.proto.service.RemoteCorfuTable.RemoteCorfuTableResponseMsg;
import org.corfudb.runtime.proto.service.RemoteCorfuTable.RemoteCorfuTableContainsKeyRequestMsg;
import org.corfudb.runtime.proto.service.RemoteCorfuTable.RemoteCorfuTableContainsResponseMsg;
import org.corfudb.runtime.proto.service.RemoteCorfuTable.RemoteCorfuTableGetRequestMsg;
import org.corfudb.runtime.proto.service.RemoteCorfuTable.RemoteCorfuTableGetResponseMsg;
import org.corfudb.runtime.proto.service.RemoteCorfuTable.RemoteCorfuTableContainsValueRequestMsg;
import org.corfudb.runtime.proto.service.RemoteCorfuTable.RemoteCorfuTableScanRequestMsg;
import org.corfudb.runtime.proto.service.RemoteCorfuTable.RemoteCorfuTableScanResponseMsg;
import org.corfudb.runtime.proto.service.RemoteCorfuTable.RemoteCorfuTableSizeRequestMsg;
import org.corfudb.runtime.proto.service.RemoteCorfuTable.RemoteCorfuTableSizeResponseMsg;
import org.corfudb.runtime.proto.service.RemoteCorfuTable.RemoteCorfuTableRequestMsg;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * This class provides methods for creating the Protobuf objects defined
 * in remote_corfu_table.proto
 *
 * <p>Created by nvaishampayan517 on 8/5/21.
 */
public class CorfuProtocolRemoteCorfuTable {
    // Prevent class from being instantiated
    private CorfuProtocolRemoteCorfuTable() {}

    /**
     * Returns a REMOTE CORFU TABLE request message containing a RemoteCorfuTableGetRequestMsg
     * that can be sent by the client. Used to request values from the server side database backing
     * the RemoteCorfuTable.
     * @param streamID The UUID of the stream backing the RemoteCorfuTable.
     * @param versionedKey The versioned key
     * @return GET request payload message to send to server
     */
    public static RequestPayloadMsg getGetRequestMsg(@NonNull UUID streamID,
                                                     @NonNull RemoteCorfuTableVersionedKey versionedKey) {
        return RequestPayloadMsg.newBuilder()
                .setRemoteCorfuTableRequest(RemoteCorfuTableRequestMsg.newBuilder()
                    .setGet(RemoteCorfuTableGetRequestMsg.newBuilder()
                        .setStreamID(getUuidMsg(streamID))
                        .setVersionedKey(versionedKey.getEncodedVersionedKey())
                        .build())
                    .build())
                .build();
    }

    /**
     * Returns a REMOTE CORFU TABLE response message containing a RemoteCorfuTableGetResponseMsg
     * that can be sent by the server. Used to return queried values from the server side database
     * backing the RemoteCorfuTable.
     * @param payloadValue Result of the GET query.
     * @return GET response payload message to send to the client.
     */
    public static ResponsePayloadMsg getGetResponseMsg(@NonNull ByteString payloadValue) {
        return ResponsePayloadMsg.newBuilder()
                .setRemoteCorfuTableResponse(RemoteCorfuTableResponseMsg.newBuilder()
                        .setGetResponse(RemoteCorfuTableGetResponseMsg.newBuilder()
                                .setPayloadValue(payloadValue)
                                .build())
                        .build())
                .build();
    }

    /**
     * Returns a REMOTE CORFU TABLE request message containing a RemoteCorfuTableScanRequestMsg
     * that can be sent by the client. Used to perform a cursor scan with the specified key
     * (usually the final key returned from the previous scan) as a starting point.
     * @param versionedStartKey Start point of cursor scan.
     * @param scanSize Amount of entries to scan for.
     * @param streamID The UUID of the stream backing the RemoteCorfuTable.
     * @return SCAN request payload message to send to the client.
     */
    public static RequestPayloadMsg getScanRequestMsg(@NonNull RemoteCorfuTableVersionedKey versionedStartKey,
                                                      @NonNegative int scanSize, @NonNull UUID streamID,
                                                      long timestamp) {
        return RequestPayloadMsg.newBuilder()
                .setRemoteCorfuTableRequest(RemoteCorfuTableRequestMsg.newBuilder()
                    .setScan(RemoteCorfuTableScanRequestMsg.newBuilder()
                        .setVersionedStartKey(versionedStartKey.getEncodedVersionedKey())
                        .setNumEntriesToScan(scanSize)
                        .setStreamID(getUuidMsg(streamID))
                        .setTimestamp(timestamp)
                        .build())
                    .build())
                .build();
    }

    /**
     * Returns a REMOTE CORFU TABLE request message containing a RemoteCorfuTableScanRequestMsg
     * that can be sent by the client. Used to perform a cursor scan starting at the first key.
     * @param scanSize Amount of entries to scan for.
     * @param streamID The UUID of the stream backing the RemoteCorfuTable.
     * @param timestamp The timestamp of the request.
     * @return SCAN request payload message to send to the server.
     */
    public static RequestPayloadMsg getScanRequestMsg(@NonNegative int scanSize, @NonNull UUID streamID,
                                                      long timestamp) {
        return RequestPayloadMsg.newBuilder()
                .setRemoteCorfuTableRequest(RemoteCorfuTableRequestMsg.newBuilder()
                        .setScan(RemoteCorfuTableScanRequestMsg.newBuilder()
                                .setTimestamp(timestamp)
                                .setNumEntriesToScan(scanSize)
                                .setStreamID(getUuidMsg(streamID))
                                .build())
                        .build())
                .build();
    }

    /**
     * Returns a REMOTE CORFU TABLE response message containing a RemoteCorfuTableScanResponseMsg
     * that can be sent by the server. Used to return scanned values to the client from the server.
     * @param entriesScanned List of all entries found from the requested scan.
     * @return SCAN response payload message to send to the client.
     */
    public static ResponsePayloadMsg getScanResponseMsg(List<RemoteCorfuTableEntry> entriesScanned) {
        return ResponsePayloadMsg.newBuilder()
                .setRemoteCorfuTableResponse(RemoteCorfuTableResponseMsg.newBuilder()
                        .setScanResponse(RemoteCorfuTableScanResponseMsg.newBuilder()
                                .addAllEntries(entriesScanned
                                        .stream()
                                        .map(CorfuProtocolRemoteCorfuTable::getEntryMsg)
                                        .collect(Collectors.toList()))
                                .build())
                        .build())
                .build();
    }

    /**
     * Returns a REMOTE CORFU TABLE request message containing a RemoteCorfuTableContainsKeyRequestMsg
     * that can be sent by the client. Used to check if a key exists in the server side database.
     * @param streamID The UUID of the stream backing the RemoteCorfuTable.
     * @param versionedKey The versioned key to check in the database.
     * @return CONTAINSKEY request payload message to send to the server.
     */
    public static RequestPayloadMsg getContainsKeyRequestMsg(@NonNull UUID streamID,
                                                             @NonNull RemoteCorfuTableVersionedKey versionedKey) {
        return RequestPayloadMsg.newBuilder()
                .setRemoteCorfuTableRequest(RemoteCorfuTableRequestMsg.newBuilder()
                    .setContainsKey(RemoteCorfuTableContainsKeyRequestMsg.newBuilder()
                        .setStreamID(getUuidMsg(streamID))
                        .setVersionedKey(versionedKey.getEncodedVersionedKey())
                        .build())
                    .build())
                .build();
    }

    /**
     * Returns a REMOTE CORFU TABLE request message containing a RemoteCorfuTableContainsValueRequestMsg
     * that can be sent by the client. Used to check if a value exists in the server side database.
     * @param streamID The UUID of the stream backing the RemoteCorfuTable.
     * @param payloadValue The value to check in the database.
     * @param timestamp The timestamp of the request.
     * @param scanSize The size of the internal scan performed.
     * @return CONTAINSVALUE request payload message to send to the server.
     */
    public static RequestPayloadMsg getContainsValueRequestMsg(@NonNull ByteString payloadValue, @NonNull UUID streamID,
                                                               long timestamp, @Positive int scanSize) {
        return RequestPayloadMsg.newBuilder()
                .setRemoteCorfuTableRequest(RemoteCorfuTableRequestMsg.newBuilder()
                    .setContainsValue(RemoteCorfuTableContainsValueRequestMsg.newBuilder()
                        .setPayloadValue(payloadValue)
                        .setStreamID(getUuidMsg(streamID))
                        .setTimestamp(timestamp)
                        .setInternalScanSize(scanSize)
                        .build())
                    .build())
                .build();
    }

    /**
     * Returns a REMOTE CORFU TABLE response message containing a RemoteCorfuTableContainsResponseMsg.
     * Used to indicate to the client whether a requested key/value exists in the server side database.
     * @param contained True, if the requested key/value exists in the database.
     * @return CONTAINS response payload message to send to the client.
     */
    public static ResponsePayloadMsg getContainsResponseMsg(boolean contained) {
        return ResponsePayloadMsg.newBuilder()
                .setRemoteCorfuTableResponse(RemoteCorfuTableResponseMsg.newBuilder()
                    .setContainsResponse(RemoteCorfuTableContainsResponseMsg.newBuilder()
                        .setContains(contained)
                        .build())
                    .build())
                .build();
    }

    /**
     * Returns a REMOTE CORFU TABLE request message containing a RemoteCorfuTableSizeRequestMsg.
     * Used to request the size of the table at the given time.
     * @param streamID The UUID of the stream backing the RemoteCorfuTable.
     * @param timestamp The timestamp of the request.
     * @param scanSize The size of the internal scan performed.
     * @return SIZE request payload message to send to the server.
     */
    public static RequestPayloadMsg getSizeRequestMsg(@NonNull UUID streamID, long timestamp, int scanSize) {
        return RequestPayloadMsg.newBuilder()
                .setRemoteCorfuTableRequest(RemoteCorfuTableRequestMsg.newBuilder()
                    .setSize(RemoteCorfuTableSizeRequestMsg.newBuilder()
                        .setStreamID(getUuidMsg(streamID))
                        .setTimestamp(timestamp)
                        .setInternalScanSize(scanSize)
                        .build())
                    .build())
                .build();
    }

    /**
     * Returns a REMOTE CORFU TABLE response message containing a RemoteCorfuTableSizeResponseMsg.
     * Used to return the results of a size request to the client.
     * @param size The size of the table at the time of the size request.
     * @return SIZE response payload message to send ot the client.
     */
    public static ResponsePayloadMsg getSizeResponseMsg(int size) {
        return ResponsePayloadMsg.newBuilder()
                .setRemoteCorfuTableResponse(RemoteCorfuTableResponseMsg.newBuilder()
                    .setSizeResponse(RemoteCorfuTableSizeResponseMsg.newBuilder()
                        .setSize(size)
                        .build())
                    .build())
                .build();
    }

    private static RemoteCorfuTableEntryMsg getEntryMsg(RemoteCorfuTableEntry entry) {
        return RemoteCorfuTableEntryMsg.newBuilder()
                .setVersionedKey(entry.getKey().getEncodedVersionedKey())
                .setPayloadValue(entry.getValue())
                .build();
    }

    /**
     * Creates a RemoteCorfuTableEntry from its protobuf representation.
     * @param msg Protobuf representation of RemoteCorfuTableEntry
     * @return The contained RemoteCorfuTableEntry
     */
    public static RemoteCorfuTableEntry getEntryFromMsg(RemoteCorfuTableEntryMsg msg) {
        return new RemoteCorfuTableEntry(
                new RemoteCorfuTableVersionedKey(msg.getVersionedKey().toByteArray()), msg.getPayloadValue());
    }

    /**
     * Creates a ContainsResponse data object from its protobuf representation
     * @param msg Contains Response Protobuf entry
     * @return ContainsResponse object from protobuf
     */
    public static ContainsResponse getContainsResponse(RemoteCorfuTableContainsResponseMsg msg) {
        return new ContainsResponse(msg.getContains());
    }

    /**
     * Creates a GetResponse data object from its protobuf representation
     * @param msg Get Response Protobuf entry
     * @return GetResponse object from protobuf
     */
    public static GetResponse getGetResponse(RemoteCorfuTableGetResponseMsg msg) {
        return new GetResponse(msg.getPayloadValue());
    }

    /**
     * Creates a ScanResponse data object from its protobuf representation
     * @param msg Scan Response Protobuf entry
     * @return ScanResponse object from protobuf
     */
    public static ScanResponse getScanResponse(RemoteCorfuTableScanResponseMsg msg) {
        return new ScanResponse(msg.getEntriesList().stream()
                .map(CorfuProtocolRemoteCorfuTable::getEntryFromMsg).collect(Collectors.toList()));
    }

    /**
     * Creates a SizeResponse data object from its protobuf representation
     * @param msg Size Response Protobuf entry
     * @return SizeResponse object from protobuf
     */
    public static SizeResponse getSizeResponse(RemoteCorfuTableSizeResponseMsg msg) {
        return new SizeResponse(msg.getSize());
    }
}

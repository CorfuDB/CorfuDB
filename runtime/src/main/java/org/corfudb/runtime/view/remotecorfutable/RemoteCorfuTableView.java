package org.corfudb.runtime.view.remotecorfutable;

import com.google.protobuf.ByteString;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableDatabaseEntry;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableVersionedKey;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.view.AbstractView;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.util.CFUtils;

import javax.annotation.Nonnegative;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class contains wrappers around Remote Corfu Table RPCs, and contains the logic for routing these
 * requests to the correct server nodes.
 *
 * Created by nvaishampayan517 on 08/16/21
 */
@Slf4j
public class RemoteCorfuTableView extends AbstractView {

    public static final String STREAM_ID_TAG = "streamId";
    public static final String NODE_SELECT_FAILURE_MSG = "Failed to route request to correct node. Cause: {}";
    public static final String ROUTING_RETRY_MSG = "Attempt #{} to route to correct node failed with {}";
    public static final String SCAN_METRIC_NAME = "remotecorfutable.scan";
    //TODO: configure in params
    private static final int ROUTING_RETRIES = 5;

    /**
     * Constructs a RemoteCorfuTableView object.
     * @param runtime Runtime associated with the view.
     */
    public RemoteCorfuTableView(@NonNull CorfuRuntime runtime) { super(runtime); }

    /**
     * Returns the value for the specified key from the Remote Corfu Table
     * @param key The key to request from the table.
     * @param streamId The stream backing the Remote Corfu Table.
     * @return The value for the requested key.
     */
    public ByteString get(@NonNull RemoteCorfuTableVersionedKey key, @NonNull UUID streamId) {
        Supplier<ByteString> getSupplier = () ->
                layoutHelper(e -> {
                    LogUnitClient logUnitClient;
                    try {
                        logUnitClient = routeRequest(e, streamId);
                    } catch (RuntimeException ex) {
                        log.warn(NODE_SELECT_FAILURE_MSG, ex.getMessage());
                        return ByteString.EMPTY;
                    }
                    return CFUtils.getUninterruptibly(logUnitClient
                            .getRemoteCorfuTableValue(key, streamId)).getValue();
                });
        return MicroMeterUtils.time(getSupplier, "remotecorfutable.get", STREAM_ID_TAG, streamId.toString());
    }

    public List<RemoteCorfuTableDatabaseEntry> multiGet(@NonNull List<RemoteCorfuTableVersionedKey> keys, @NonNull UUID streamId) {
        Supplier<List<RemoteCorfuTableDatabaseEntry>> multiGetSupplier = () ->
                layoutHelper(e -> {
                    LogUnitClient logUnitClient;
                    try {
                        logUnitClient = routeRequest(e, streamId);
                    } catch (RuntimeException ex) {
                        log.warn(NODE_SELECT_FAILURE_MSG, ex.getMessage());
                        return keys.stream()
                                .map(key -> new RemoteCorfuTableDatabaseEntry(key, ByteString.EMPTY))
                                .collect(Collectors.toList());
                    }
                    return CFUtils.getUninterruptibly(logUnitClient
                                .multiGetRemoteCorfuTable(keys, streamId)).getEntries();
                });
        return MicroMeterUtils.time(multiGetSupplier, "remotecorfutable.multiget", STREAM_ID_TAG, streamId.toString());
    }

    /**
     * Returns a scan of the RemoteCorfuTable starting at the beginning, with the default amount of entries returned.
     * @param streamId The stream backing the Remote Corfu Table.
     * @param timestamp The timestamp of the request.
     * @return The list of scanned table entries.
     */
    public List<RemoteCorfuTableDatabaseEntry> scan(@NonNull UUID streamId, long timestamp) {
        Supplier<List<RemoteCorfuTableDatabaseEntry>> scanSupplier = () ->
                layoutHelper(e -> {
                    LogUnitClient logUnitClient;
                    try {
                        logUnitClient = routeRequest(e, streamId);
                    } catch (RuntimeException ex) {
                        log.warn(NODE_SELECT_FAILURE_MSG, ex.getMessage());
                        return new LinkedList<>();
                    }
                    return CFUtils.getUninterruptibly(logUnitClient
                                .scanRemoteCorfuTable(streamId,timestamp)).getEntries();
                });
        return MicroMeterUtils.time(scanSupplier, SCAN_METRIC_NAME, STREAM_ID_TAG, streamId.toString(),
                "type", "start-defaultsize");
    }

    /**
     * Returns a scan of the RemoteCorfuTable starting at the beginning.
     * @param scanSize The amount of entries to scan from the table.
     * @param streamId The stream backing the Remote Corfu Table.
     * @param timestamp The timestamp of the request.
     * @return The list of scanned table entries.
     */
    public List<RemoteCorfuTableDatabaseEntry> scan(@Nonnegative int scanSize, @NonNull UUID streamId, long timestamp) {
        Supplier<List<RemoteCorfuTableDatabaseEntry>> scanSupplier = () ->
                layoutHelper(e -> {
                    LogUnitClient logUnitClient;
                    try {
                        logUnitClient = routeRequest(e, streamId);
                    } catch (RuntimeException ex) {
                        log.warn(NODE_SELECT_FAILURE_MSG, ex.getMessage());
                        return new LinkedList<>();
                    }
                    return CFUtils.getUninterruptibly(logUnitClient
                            .scanRemoteCorfuTable(scanSize,streamId,timestamp)).getEntries();
                });
        return MicroMeterUtils.time(scanSupplier, SCAN_METRIC_NAME, STREAM_ID_TAG, streamId.toString(),
                "type", "start-customsize");
    }

    /**
     * Returns a scan of the RemoteCorfuTable starting at the specified start point,
     * with the default amount of entries returned.
     * @param startPoint The start point of the scan (exclusive).
     * @param streamId The stream backing the Remote Corfu Table.
     * @param timestamp The timestamp of the request.
     * @return The list of scanned table entries.
     */
    public List<RemoteCorfuTableDatabaseEntry> scan(@NonNull RemoteCorfuTableVersionedKey startPoint, @NonNull UUID streamId,
                                                    long timestamp) {
        Supplier<List<RemoteCorfuTableDatabaseEntry>> scanSupplier = () ->
                layoutHelper(e -> {
                    LogUnitClient logUnitClient;
                    try {
                        logUnitClient = routeRequest(e, streamId);
                    } catch (RuntimeException ex) {
                        log.warn(NODE_SELECT_FAILURE_MSG, ex.getMessage());
                        return new LinkedList<>();
                    }
                    return CFUtils.getUninterruptibly(logUnitClient
                                .scanRemoteCorfuTable(startPoint,streamId,timestamp)).getEntries();
                });
        return MicroMeterUtils.time(scanSupplier, SCAN_METRIC_NAME, STREAM_ID_TAG, streamId.toString(),
                "type", "continuation-defaultsize");
    }

    /**
     * Returns a scan of the RemoteCorfuTable starting at the specified start point.
     * @param startPoint The start point of the scan (exclusive).
     * @param scanSize The amount of entries to scan from the table.
     * @param streamId The stream backing the Remote Corfu Table.
     * @param timestamp The timestamp of the request.
     * @return The list of scanned table entries.
     */
    public List<RemoteCorfuTableDatabaseEntry> scan(@NonNull RemoteCorfuTableVersionedKey startPoint, @Nonnegative int scanSize,
                                                    @NonNull UUID streamId, long timestamp) {
        Supplier<List<RemoteCorfuTableDatabaseEntry>> scanSupplier = () ->
                layoutHelper(e -> {
                    LogUnitClient logUnitClient;
                    try {
                        logUnitClient = routeRequest(e, streamId);
                    } catch (RuntimeException ex) {
                        log.warn(NODE_SELECT_FAILURE_MSG, ex.getMessage());
                        return new LinkedList<>();
                    }
                    return CFUtils.getUninterruptibly(logUnitClient
                                .scanRemoteCorfuTable(startPoint, scanSize, streamId, timestamp)).getEntries();
                });
        return MicroMeterUtils.time(scanSupplier, SCAN_METRIC_NAME, STREAM_ID_TAG, streamId.toString(),
                "type", "continuation-customsize");
    }

    /**
     * Returns true if the Remote Corfu Table contains the specified key.
     * @param key The key to query in the table.
     * @param streamId The stream backing the Remote Corfu Table.
     * @return True if the key exists in the table.
     */
    public boolean containsKey(@NonNull RemoteCorfuTableVersionedKey key, @NonNull UUID streamId) {
        Supplier<Boolean> containsKeySupplier = () ->
                layoutHelper(e -> {
                    LogUnitClient logUnitClient;
                    try {
                        logUnitClient = routeRequest(e, streamId);
                    } catch (RuntimeException ex) {
                        log.warn(NODE_SELECT_FAILURE_MSG, ex.getMessage());
                        return false;
                    }
                    return CFUtils.getUninterruptibly(logUnitClient
                        .containsKeyRemoteCorfuTable(key, streamId)).isContained();
                });
        return MicroMeterUtils.time(containsKeySupplier, "remotecorfutable.containskey", STREAM_ID_TAG,
                streamId.toString());
    }

    /**
     * Returns true if the Remote Corfu Table contains the specified value.
     * @param value The value to query in the table.
     * @param streamId The stream backing the Remote Corfu Table.
     * @param timestamp The timestamp of the query.
     * @param scanSize The size of each internal database scan during the full database scan.
     * @return True if the value exists in the table
     */
    public boolean containsValue(@NonNull ByteString value, @NonNull UUID streamId, long timestamp, int scanSize) {
        Supplier<Boolean> containsValueSupplier = () ->
                layoutHelper(e -> {
                    LogUnitClient logUnitClient;
                    try {
                        logUnitClient = routeRequest(e, streamId);
                    } catch (RuntimeException ex) {
                        log.warn(NODE_SELECT_FAILURE_MSG, ex.getMessage());
                        return false;
                    }
                    return CFUtils.getUninterruptibly(logUnitClient
                        .containsValueRemoteCorfuTable(value, streamId, timestamp, scanSize)).isContained();
                });
        return MicroMeterUtils.time(containsValueSupplier, "remotecorfutable.containsvalue", STREAM_ID_TAG,
                streamId.toString());
    }

    /**
     * Returns the size of the Remote Corfu Table at the specified timestamp.
     * @param streamId The stream backing the Remote Corfu Table.
     * @param timestamp The timestamp of the query.
     * @param scanSize The size of each internal database scan during the full database scan.
     * @return The size of the table.
     */
    public int size(@NonNull UUID streamId, long timestamp, int scanSize) {
        Supplier<Integer> sizeSupplier = () ->
                layoutHelper(e -> {
                    LogUnitClient logUnitClient;
                    try {
                        logUnitClient = routeRequest(e, streamId);
                    } catch (RuntimeException ex) {
                        log.warn(NODE_SELECT_FAILURE_MSG, ex.getMessage());
                        return 0;
                    }
                    return CFUtils.getUninterruptibly(logUnitClient
                        .sizeRemoteCorfuTable(streamId, timestamp, scanSize)).getSize();
                });
        return MicroMeterUtils.time(sizeSupplier, "remotecorfutable.size", STREAM_ID_TAG,
                streamId.toString());
    }

    private LogUnitClient routeRequest(RuntimeLayout e, UUID streamId) {
        for (int i = 1; i <= ROUTING_RETRIES; i++) {
            try {
                return getNodeWithCorrectStripe(e, streamId);
            } catch (RuntimeException ex) {
                log.warn(ROUTING_RETRY_MSG, i, ex.getMessage());
            }
        }
        throw new RuntimeException("Stream tail does not exist. Retries exhausted.");
    }

    private LogUnitClient getNodeWithCorrectStripe(RuntimeLayout e, UUID streamId) {
        long streamTail = e.getRuntime().getSequencerView().query(streamId);
        return e.getLogUnitClient(streamTail, 0);
    }
}

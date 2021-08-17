package org.corfudb.runtime.view.remotecorfutable;

import com.google.protobuf.ByteString;
import lombok.NonNull;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableEntry;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableVersionedKey;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.view.AbstractView;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.util.CFUtils;

import javax.annotation.Nonnegative;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * This class contains wrappers around Remote Corfu Table RPCs, and contains the logic for routing these
 * requests to the correct server nodes.
 *
 * Created by nvaishampayan517 on 08/16/21
 */
public class RemoteCorfuTableView extends AbstractView {
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
                layoutHelper(e ->
                        CFUtils.getUninterruptibly(getNodeWithCorrectStripe(e,streamId)
                                .getRemoteCorfuTableValue(key, streamId)).getValue());
        return MicroMeterUtils.time(getSupplier, "remotecorfutable.get", "streamId", streamId.toString());
    }

    /**
     * Returns a scan of the RemoteCorfuTable starting at the beginning, with the default amount of entries returned.
     * @param streamId The stream backing the Remote Corfu Table.
     * @param timestamp The timestamp of the request.
     * @return The list of scanned table entries.
     */
    public List<RemoteCorfuTableEntry> scan(@NonNull UUID streamId, long timestamp) {
        Supplier<List<RemoteCorfuTableEntry>> scanSupplier = () ->
                layoutHelper(e ->
                        CFUtils.getUninterruptibly(getNodeWithCorrectStripe(e, streamId)
                                .scanRemoteCorfuTable(streamId,timestamp)).getEntries());
        return MicroMeterUtils.time(scanSupplier, "remotecorfutable.scan", "streamId", streamId.toString(),
                "type", "start-defaultsize");
    }

    /**
     * Returns a scan of the RemoteCorfuTable starting at the beginning.
     * @param scanSize The amount of entries to scan from the table.
     * @param streamId The stream backing the Remote Corfu Table.
     * @param timestamp The timestamp of the request.
     * @return The list of scanned table entries.
     */
    public List<RemoteCorfuTableEntry> scan(@Nonnegative int scanSize, @NonNull UUID streamId, long timestamp) {
        Supplier<List<RemoteCorfuTableEntry>> scanSupplier = () ->
                layoutHelper(e ->
                        CFUtils.getUninterruptibly(getNodeWithCorrectStripe(e, streamId)
                                .scanRemoteCorfuTable(scanSize,streamId,timestamp)).getEntries());
        return MicroMeterUtils.time(scanSupplier, "remotecorfutable.scan", "streamId", streamId.toString(),
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
    public List<RemoteCorfuTableEntry> scan(@NonNull RemoteCorfuTableVersionedKey startPoint, @NonNull UUID streamId,
                                            long timestamp) {
        Supplier<List<RemoteCorfuTableEntry>> scanSupplier = () ->
                layoutHelper(e ->
                        CFUtils.getUninterruptibly(getNodeWithCorrectStripe(e, streamId)
                                .scanRemoteCorfuTable(startPoint,streamId,timestamp)).getEntries());
        return MicroMeterUtils.time(scanSupplier, "remotecorfutable.scan", "streamId", streamId.toString(),
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
    public List<RemoteCorfuTableEntry> scan(@NonNull RemoteCorfuTableVersionedKey startPoint, @Nonnegative int scanSize,
                                            @NonNull UUID streamId, long timestamp) {
        Supplier<List<RemoteCorfuTableEntry>> scanSupplier = () ->
                layoutHelper(e ->
                        CFUtils.getUninterruptibly(getNodeWithCorrectStripe(e, streamId)
                                .scanRemoteCorfuTable(startPoint, scanSize, streamId, timestamp)).getEntries());
        return MicroMeterUtils.time(scanSupplier, "remotecorfutable.scan", "streamId", streamId.toString(),
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
                layoutHelper(e ->
                        CFUtils.getUninterruptibly(getNodeWithCorrectStripe(e, streamId)
                        .containsKeyRemoteCorfuTable(key, streamId)).isContained());
        return MicroMeterUtils.time(containsKeySupplier, "remotecorfutable.containskey", "streamId",
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
                layoutHelper(e ->
                        CFUtils.getUninterruptibly(getNodeWithCorrectStripe(e, streamId)
                                .containsValueRemoteCorfuTable(value, streamId, timestamp, scanSize)).isContained());
        return MicroMeterUtils.time(containsValueSupplier, "remotecorfutable.containsvalue", "streamId",
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
                layoutHelper(e ->
                        CFUtils.getUninterruptibly(getNodeWithCorrectStripe(e, streamId)
                        .sizeRemoteCorfuTable(streamId, timestamp, scanSize)).getSize());
        return MicroMeterUtils.time(sizeSupplier, "remotecorfutable.size", "streamId",
                streamId.toString());
    }

    private LogUnitClient getNodeWithCorrectStripe(RuntimeLayout e, UUID streamId) {
        long streamTail = e.getRuntime().getSequencerView().query(streamId);
        return e.getLogUnitClient(streamTail, 0);
    }
}

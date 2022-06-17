package org.corfudb.infrastructure.management.failuredetector;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.datastore.DataStore;
import org.corfudb.infrastructure.datastore.KvDataStore;
import org.corfudb.protocols.wireprotocol.failuredetector.FailureDetectorMetrics;
import org.corfudb.runtime.view.Layout;

import java.util.Optional;

/**
 * Manages failure detector related information
 */
@Slf4j
@Builder
public class FailureDetectorDataStore {
    // Failure detector
    private static final String PREFIX_FAILURE_DETECTOR = "FAILURE_DETECTOR";

    @NonNull
    private final DataStore dataStore;

    @NonNull
    private final String localEndpoint;

    /**
     * Save detected failure in a history. History represents all cluster state changes.
     * Disabled by default.
     *
     * @param detector failure detector state
     */
    public synchronized void saveFailureDetectorMetrics(FailureDetectorMetrics detector) {
        boolean enabled = Boolean.parseBoolean(System.getProperty("corfu.failuredetector", Boolean.FALSE.toString()));
        if (!enabled) {
            return;
        }

        KvDataStore.KvRecord<FailureDetectorMetrics> fdRecord = KvDataStore.KvRecord.of(
                PREFIX_FAILURE_DETECTOR,
                String.valueOf(getManagementLayout().getEpoch()),
                FailureDetectorMetrics.class
        );

        dataStore.put(fdRecord, detector);
    }

    /**
     * Get latest change in a cluster state saved in data store. Or provide default value if history is disabled.
     *
     * @return latest failure saved in the history
     */
    public FailureDetectorMetrics getFailureDetectorMetrics() {
        boolean enabled = Boolean.parseBoolean(System.getProperty("corfu.failuredetector", Boolean.FALSE.toString()));
        if (!enabled) {
            return getDefaultFailureDetectorMetric(getManagementLayout());
        }

        KvDataStore.KvRecord<FailureDetectorMetrics> fdRecord = KvDataStore.KvRecord.of(
                PREFIX_FAILURE_DETECTOR,
                String.valueOf(getManagementLayout().getEpoch()),
                FailureDetectorMetrics.class
        );

        return Optional
                .ofNullable(dataStore.get(fdRecord))
                .orElseGet(() -> getDefaultFailureDetectorMetric(getManagementLayout()));
    }

    /**
     * Fetches the management layout from the persistent datastore.
     *
     * @return The last persisted layout
     */
    public Layout getManagementLayout() {
        return dataStore.get(ServerContext.MANAGEMENT_LAYOUT_RECORD);
    }

    /**
     * Provide default metric.
     *
     * @param layout current layout
     * @return default value
     */
    private FailureDetectorMetrics getDefaultFailureDetectorMetric(Layout layout) {
        return FailureDetectorMetrics.builder()
                .localNode(localEndpoint)
                .layout(layout.getLayoutServers())
                .unresponsiveNodes(layout.getUnresponsiveServers())
                .epoch(layout.getEpoch())
                .build();
    }
}

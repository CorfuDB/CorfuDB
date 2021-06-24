package org.corfudb.runtime.collections;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.exceptions.StreamingException;

import java.util.List;

/**
 * This is an abstract stream listener callback implementation for clients interested in automatic
 * re-subscription upon any error. The re-subscription policy of this implementation resumes streaming
 * from the last processed entry (optimization). If and only if subscription can't be resumed
 * (for instance, TrimmedException) we perform a full sync (snapshot) and re-subscribe from that point onwards.
 * This implementation protects against data loss.
 *
 * Extending from this class requires implementing: 'onNext' (callback for new records) and 'performFullSync' methods.
 *
 * Created by annym on 06-22-2021
 */
@Slf4j
public abstract class StreamListenerResumeOrFullSync extends StreamListenerResumePolicy {

    public StreamListenerResumeOrFullSync(CorfuStore store, String namespace, String streamTag, List<String> tablesOfInterest) {
        super(store, namespace, streamTag, tablesOfInterest);
    }

    public StreamListenerResumeOrFullSync(CorfuStore store, String namespace, String streamTag) {
        this(store, namespace, streamTag, null);
    }

    /**
     * Perform full sync (snapshot) on a table or tables of interest (the ones subscribed to).
     *
     * Note that, the timestamp returned by performFullSync will be the continuity point for delta subscription.
     * If more than one table is going to be full synced, consider the following:
     *   - Perform a transaction across all tables to guarantee the snapshot timestamp is valid for all tables.
     *   This will be the simplest approach.
     *   - If you decide to do a full sync on each table separately (i.e., separate transactions) you will
     *  need to return the min of all snapshot timestamps. However, consider that in this case you might process
     *  duplicates, as data coming from a full sync for a given table might as well show up as deltas, so you must
     *  be resilient to these cases.
     *
     * @return timestamp from which to subscribe for deltas.
     * Note: this timestamp should come from txn.commit() of the full sync.
     */
    protected abstract Timestamp performFullSync();

    @Override
    public void subscribeOnResumeError() {
        try {
            Timestamp fullSyncTimestamp = performFullSync();

            if (tablesOfInterest == null) {
                store.subscribeListener(this, namespace, streamTag, fullSyncTimestamp);
            } else {
                store.subscribeListener(this, namespace, streamTag, tablesOfInterest, fullSyncTimestamp);
            }
        } catch (StreamingException se) {
            log.error("Failed to subscribe listener [tag:{}] {}$[{}]", streamTag, namespace, tablesOfInterest);
        } catch (Exception e) {
            log.error("Failed to perform full sync [tag:{}] {}$[{}]. Listener is NOT SUBSCRIBED!", streamTag, namespace,
                    tablesOfInterest);
        }
    }
}

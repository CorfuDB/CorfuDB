package org.corfudb.runtime.collections;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.exceptions.StreamingException;
import org.corfudb.runtime.exceptions.UnreachableClusterException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.util.List;

/**
 * This is an abstract stream listener callback implementation for clients interested in automatic
 * re-subscription upon any error. 
 *
 * Created by annym on 06-22-2021
 */
@Slf4j
abstract class StreamListenerResumePolicy implements StreamListener {

    protected final CorfuStore store;
    protected final String namespace;
    protected final String streamTag;
    protected final List<String> tablesOfInterest;
    private Timestamp lastProcessedEntryTs;

    public StreamListenerResumePolicy(CorfuStore store, String namespace, String streamTag, List<String> tablesOfInterest) {
        this.store = store;
        this.namespace = namespace;
        this.streamTag = streamTag;
        this.tablesOfInterest = tablesOfInterest;
    }

    public StreamListenerResumePolicy(CorfuStore store, String namespace, String streamTag) {
        this(store, namespace, streamTag, null);
    }

    /**
     * Implementation of re-subscription policy in case resuming subscription from last processed entry fails.
     */
    public abstract void subscribeOnResumeError();

    public void resumeSubscription() {
        if (lastProcessedEntryTs == null) {
            log.warn("Last processed entry timestamp has not been set, failed to resume subscription." +
                    " Default to fallback subscription.");
            subscribeOnResumeError();
            return;
        }

        try {
            log.info("Resume subscription on [tag:{}] {}$[{}] from {}", streamTag, namespace, tablesOfInterest,
                    lastProcessedEntryTs);

            if (tablesOfInterest == null) {
                store.subscribeListener(this, namespace, streamTag, lastProcessedEntryTs);
            } else {
                store.subscribeListener(this, namespace, streamTag, tablesOfInterest, lastProcessedEntryTs);
            }
        } catch (StreamingException e) {
            log.error("Failed to resume subscription [tag:{}] from last processed entry {}. Re-subscribe based on " +
                    "implemented fallback.", streamTag, lastProcessedEntryTs, e);
            this.subscribeOnResumeError();
        }
    }

    @Override
    public void onNextEntry(CorfuStreamEntries results) {
        onNext(results);
        this.lastProcessedEntryTs = results.getTimestamp();
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Exception caught during streaming processing. Re-subscribe this listener to latest timestamp. " +
                "Error=", throwable);

        if (throwable instanceof UnreachableClusterException) {
            try {
                IRetry.build(IntervalRetry.class, () -> {
                    try {
                        log.info("Subscribe onError");
                        resumeSubscription();
                    } catch (UnreachableClusterException e) {
                        log.error("Error while attempting to re-subscribe listener after onError.", e);
                        throw new RetryNeededException();
                    }
                    return null;
                }).run();
            } catch (InterruptedException e) {
                log.error("Unrecoverable exception when attempting to re-subscribe listener.", e);
                throw new UnrecoverableCorfuInterruptedError(e);
            }
        } else {
            resumeSubscription();
        }
    }
}

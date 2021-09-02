package org.corfudb.runtime.collections;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.StreamingException;

import java.util.List;

/**
 * This is an abstract stream listener callback implementation for clients
 * interested in automatic re-subscription upon any error.
 * The re-subscription policy of this implementation is an attempt to
 * optimize and resume streaming after the last processed entry. If and only if
 * we can't resume from the last processed entry, we subscribe from the latest
 * position in the log (default), which can incur in data loss, as the latest
 * position in the log might have progressed beyond several updates from
 * the last processed entry.
 *
 * Please note if data loss is a concern DO NOT USE this listener implementation
 * and refer to StreamListenerResumeOrFullSync
 *
 * Created by annym on 06-22-2021
 */
@Slf4j
public abstract class StreamListenerResumeOrDefault extends StreamListenerResumePolicy {

    public StreamListenerResumeOrDefault(CorfuStore store, String namespace, String streamTag,
                                         List<String> tablesOfInterest) {
        super(store, namespace, streamTag, tablesOfInterest);
    }

    public StreamListenerResumeOrDefault(CorfuStore store, String namespace, String streamTag) {
        this(store, namespace, streamTag, null);
    }

    /**
     * This method contains the subscription policy in case the attempt to 'resume' streaming from
     * the last processed entry fails. In this case we subscribe from the latest position in the log
     * (which can incur in data loss, i.e., updates between last processed entry and latest position in the log
     * would be lost).
     */
    @Override
    public void subscribeOnResumeError() {
        // Immediately re-subscribe from the latest point in the log, this means there is no interest in
        // keeping continuity with the last processed entry before an error was thrown.
        // Be aware that this re-subscription pattern will likely lead to data loss, i.e., you will lose any
        // data between the last processed entry and the current log tail.
        try {
            if (tablesOfInterest == null) {
                store.subscribeListener(this, namespace, streamTag);
            } else {
                store.subscribeListener(this, namespace, streamTag, tablesOfInterest);
            }
        } catch (StreamingException e) {
            log.error("Failed to subscribe listener [tag:{}] {}$[{}]", streamTag, namespace, tablesOfInterest);
        }
    }
}

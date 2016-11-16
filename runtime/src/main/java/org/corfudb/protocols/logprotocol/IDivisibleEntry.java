package org.corfudb.protocols.logprotocol;

import java.util.UUID;

/**
 * This is an interface for divisible log entries, that is,
 * log entries which can be divided into their per-stream components.
 * Created by mwei on 9/20/16.
 */
public interface IDivisibleEntry {

    /** Produce an entry for the given stream, or return null if there is no entry.
     *
     * @param stream    The stream to produce an entry for.
     * @return          An entry for that stream, or NULL, if there is no entry for that stream.
     */
    LogEntry divideEntry(UUID stream);
}

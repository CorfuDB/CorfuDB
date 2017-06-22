package org.corfudb.protocols.logprotocol;

import java.util.UUID;

/** This class represents a stream data, which is data for a stream contained
 * in a single global address. There may be at most one stream data per stream
 * per global address.
 *
 * <p>Created by mwei on 4/6/17.
 */
public class StreamData {

    /** The id of the stream this stream data belongs to. */
    UUID streamId;

    /** The backpointer this stream data. */
    Long backpointer;

    /** The log entry (to be renamed stream entry) at this stream data. */
    LogEntry streamEntry;
}

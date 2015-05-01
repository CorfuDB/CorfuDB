package org.corfudb.runtime.stream;

import org.corfudb.runtime.OutOfSpaceException;
import org.corfudb.runtime.TrimmedException;
import org.corfudb.runtime.UnwrittenException;

import java.io.IOException;
import java.io.Serializable;

/**
 * This interface represents an append-only stream. The basic operations of an append-only stream are:
 *
 * append, which places a new entry at the tail of the stream
 * read, which allows for random reads
 * trim, which trims a prefix of the stream, exclusive
 *
 * Created by mwei on 4/30/15.
 */
public interface ILog {

    /**
     * Appends data to the tail (end) of the log.
     * @param data                      The object to append to tail of the log.
     * @return                          A timestamp representing the address the data was written to.
     * @throws OutOfSpaceException      If there is no space remaining on the log.
     */
    ITimestamp append(Serializable data)
            throws OutOfSpaceException;

    /**
     * Reads data from an address in the log.
     * @param address                   A timestamp representing the location to read from.
     * @return                          The object at that address.
     * @throws UnwrittenException       If the address was unwritten.
     * @throws TrimmedException         If the address has been trimmed.
     */
    Object read(ITimestamp address)
            throws UnwrittenException, TrimmedException, ClassNotFoundException, IOException;

    /**
     * Trims a prefix of the log, [0, address) exclusive.
     * @param address                   A timestamp representing the location to trim.
     */
    void trim(ITimestamp address);
}

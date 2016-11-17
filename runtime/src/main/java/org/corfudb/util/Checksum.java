package org.corfudb.util;

/**
 * An interface representing a checksum for a sequence of bytes.
 *
 */

public interface Checksum {

    /**
     * Returns the current checksum value.
     * @return the current checksum value
     */
    byte[] getValue();

    /**
     * Returns the size of the checksum's value in bytes
     * @return Returns the size of the checksum's value in bytes
     */
    int getSize();
}

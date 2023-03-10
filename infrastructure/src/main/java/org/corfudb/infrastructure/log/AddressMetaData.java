package org.corfudb.infrastructure.log;

/**
 * Created by maithem on 3/14/17.
 */
public class AddressMetaData {
    public final int length;
    public final long offset;

    /**
     * Returns a metadata object for an address.
     *
     * @param length    length of log data
     * @param offset    file channel offset
     **/
    public AddressMetaData(int length, long offset) {
        this.length = length;
        this.offset = offset;
    }
}

package org.corfudb.infrastructure.log;

/**
 * Created by maithem on 3/14/17.
 */
public class AddressMetaData {
    public final long globalAddress;
    public final int checksum;
    public final int length;
    public final long offset;

    /**
     * Returns a metadata object for an address.
     *
     * @param checksum  checksum of log data
     * @param length    length of log data
     * @param offset    file channel offset
     **/
    public AddressMetaData(long globalAddress, int checksum, int length, long offset) {
        this.globalAddress = globalAddress;
        this.checksum = checksum;
        this.length = length;
        this.offset = offset;
    }
}

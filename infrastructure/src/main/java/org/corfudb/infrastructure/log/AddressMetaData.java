package org.corfudb.infrastructure.log;

/**
 * Created by maithem on 3/14/17.
 */
public class AddressMetaData {
    public final int checksum;
    public final int length;
    public final long offset;

    public AddressMetaData(int checksum, int length, long offset) {
        this.checksum = checksum;
        this.length = length;
        this.offset = offset;
    }
}

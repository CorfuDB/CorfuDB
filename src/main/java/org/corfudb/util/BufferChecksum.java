package org.corfudb.util;

import java.util.Arrays;

/**
 *
 * Implementation of Checksum that allows variable length checksum values.
 *
 * Created by box on 10/30/16.
 */

public class BufferChecksum implements Checksum {
    private byte[] value;

    public BufferChecksum(byte[] value){
        this.value = value;
    }

    public byte[] getValue(){
        return value;
    }

    public int getSize(){
        return value.length;
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof Checksum) && Arrays.equals(this.value, ((Checksum) o).getValue());
    }
}

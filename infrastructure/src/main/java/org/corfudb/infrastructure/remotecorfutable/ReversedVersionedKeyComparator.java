package org.corfudb.infrastructure.remotecorfutable;

import org.rocksdb.AbstractComparator;
import org.rocksdb.ComparatorOptions;

import java.nio.ByteBuffer;

public class ReversedVersionedKeyComparator extends AbstractComparator {
    protected ReversedVersionedKeyComparator(ComparatorOptions comparatorOptions) {
        super(comparatorOptions);
    }

    private final String name = "corfudb.remotecorfutable.reversedversionedkeycomparator";
    @Override
    public String name() {
        return name;
    }

    @Override
    public int compare(ByteBuffer a, ByteBuffer b) {
        int i = 0;
        int aLen = a.remaining();
        int bLen = b.remaining();
        //all keys have 8 byte timestamp at end
        int endPoint = (aLen > bLen) ? bLen - 8 : aLen - 8;
        int comparison;
        //compare keys bytewise
        for(; i < endPoint; i++) {
            //perform comparison as unsigned bytes
            //we want reverse comparator, so subtract a from b
            comparison = (b.get(i) & 0xff) - (a.get(i) & 0xff);
            if (comparison != 0) {
                return comparison;
            }
        }
        if (aLen == bLen) {
            //we want reverse comparator, reverse args
            return Long.compareUnsigned(b.getLong(i), a.getLong(i));
        } else {
            //we want reverse comparator, so subtract a from b
            return bLen - aLen;
        }
    }
}

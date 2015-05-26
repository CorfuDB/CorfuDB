package org.corfudb.runtime.stream;

import java.io.Serializable;

/**
 * This is a simple timestamp which depends on a single global monotonically
 * increasing counter. Timestamps across streams can be compared.
 *
 * Created by mwei on 4/30/15.
 */
public class SimpleTimestamp implements ITimestamp, Serializable {

    public long address;

    public SimpleTimestamp(long address)
    {
        this.address = address;
    }

    @Override
    public String toString()
    {
        return Long.toString(address);
    }

    /**
     * Default comparator for ITimestamp.
     *
     * @param timestamp
     */
    @Override
    public int compareTo(ITimestamp timestamp) {
        //always less than max
        if (ITimestamp.isMax(timestamp)) { return -1; }
        //always greater than min
        if (ITimestamp.isMin(timestamp)) { return 1; }
        //always invalid
        if (ITimestamp.isInvalid(timestamp)) { throw new ClassCastException("Comparison of invalid timestamp!"); }

        if (timestamp instanceof SimpleTimestamp)
        {
            return Long.compare(this.address, ((SimpleTimestamp) timestamp).address);
        }
        throw new ClassCastException("Unknown timestamp!");
    }

    @Override
    public boolean equals(Object o)
    {
        return (o instanceof SimpleTimestamp) && address == ((SimpleTimestamp)o).address;
    }

    @Override
    public int hashCode()
    {
        return (int) address;
    }
}

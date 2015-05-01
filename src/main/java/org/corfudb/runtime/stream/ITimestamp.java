package org.corfudb.runtime.stream;

/**
 *  This interface represents comparable timestamps with max, min and invalid values.
 */
public interface ITimestamp extends Comparable<ITimestamp>
{
    /**
     * Gets an invalid timestamp. Comparisons against this timestamp will always be invalid.
     *
     * @ @return        An invalid timestamp.
     */
    static ITimestamp getInvalidTimestamp() {
        return ITimestampConstant.getInvalid();
    }

    /**
     * Gets the maximum timestamp. No timestamp should be greater than this timestamp.
     *
     * @return          A maximum timestamp.
     */
    static ITimestamp getMaxTimestamp() {
        return ITimestampConstant.getMax();
    }

    /**
     * Gets the minimum timestamp. No timestamp should be less than this timestamp.
     *
     * @return          A minimum timestamp.
     */
    static ITimestamp getMinTimestamp() {
        return ITimestampConstant.getMin();
    }

    /** Checks if the timestamp is the maximum timestamp.
     *
     * @param timestamp The timestamp to check.
     *
     * @return          True is the timestamp is a max timestamp, false otherwise.
     */
    static boolean isMax(ITimestamp timestamp)
    {
        if (timestamp instanceof ITimestampConstant)
        {
            return ((ITimestampConstant)timestamp).isMax();
        }
        return false;
    }

    /** Checks if the timestamp is the minimum timestamp.
     *
     * @param timestamp The timestamp to check.
     *
     * @return          True is the timestamp is a min timestamp, false otherwise.
     */
    static boolean isMin(ITimestamp timestamp)
    {
        if (timestamp instanceof ITimestampConstant)
        {
            return ((ITimestampConstant)timestamp).isMin();
        }
        return false;
    }

    /** Checks if the timestamp is the invalid timestamp.
     *
     * @param timestamp The timestamp to check.
     *
     * @return          True is the timestamp is an invalid timestamp, false otherwise.
     */
    static boolean isInvalid(ITimestamp timestamp)
    {
        if (timestamp instanceof ITimestampConstant)
        {
            return ((ITimestampConstant)timestamp).isInvalid();
        }
        return false;
    }

    /**
     * Default comparator for ITimestamp.
     *
     */
    @Override
    public default int compareTo(ITimestamp timestamp)
    {
        // Take care of equality
        if (this.equals(timestamp)) {return 0;}

        if (timestamp instanceof ITimestampConstant)
        {
            ITimestampConstant c = (ITimestampConstant) this;
            if (c.isInvalid()) { throw new ClassCastException("Comparison of invalid timestamp!"); }
            if (c.isMax()) { return -1; }
            if (c.isMin()) { return 1; }
        }

        if (this instanceof ITimestampConstant)
        {
            ITimestampConstant c = (ITimestampConstant) this;
            if (c.isInvalid()) { throw new ClassCastException("Comparison of invalid timestamp!"); }
            if (c.isMax()) { return 1; }
            if (c.isMin()) { return -1; }
        }

        throw new ClassCastException("I don't know how to compare these timestamps, (maybe you need to override comapreTo<ITimestamp> in your timestamp implementation?) [ts1=" + this.toString() + "] [ts2=" + timestamp.toString()+"]");
    }
}

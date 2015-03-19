package org.corfudb.client;

/**
 *  This class represents constants for ITimestamp.
 */
public class ITimestampConstant implements ITimestamp
{
    /**
     * This enumeration provides set constants for ITimestamp.
     */
    enum ITimestampConstantValue {
        /** A maximum value timestamp */
        MAX_VALUE,
        /** A minimum value timestamp */
        MIN_VALUE,
        /** A invalid value timestamp */
        INVALID_VALUE
    }

    /** The value of the timestamp */
    ITimestampConstantValue value;

    /**
     * Constructs a new timestamp based on the value enumeration.
     *
     * @param value         The value of the timestamp.
     */
    public ITimestampConstant(ITimestampConstantValue value) {
        this.value = value;
    }

    /**
     * Gets the maximum timestamp. No timestamp should be greater than this timestamp.
     *
     * @return          A maximum timestamp.
     */
    static ITimestampConstant getMax()
    {
        return new ITimestampConstant(ITimestampConstantValue.MAX_VALUE);
    }

    /**
     * Gets the minimum timestamp. No timestamp should be less than this timestamp.
     *
     * @return          A minimum timestamp.
     */
    static ITimestampConstant getMin()
    {
        return new ITimestampConstant(ITimestampConstantValue.MIN_VALUE);
    }

    /**
     * Gets an invalid timestamp. Comparisons against this timestamp will always be invalid.
     *
     * @ @return        An invalid timestamp.
     */
    static ITimestampConstant getInvalid()
    {
        return new ITimestampConstant(ITimestampConstantValue.INVALID_VALUE);
    }

    /** Checks if the timestamp is the maximum timestamp.
     *
     * @return          True is the timestamp is a max timestamp, false otherwise.
     */
    public boolean isMax()
    {
        return value == ITimestampConstantValue.MAX_VALUE;
    }

    /** Checks if the timestamp is the minimum timestamp.
     *
     * @return          True is the timestamp is a min timestamp, false otherwise.
     */
    public boolean isMin()
    {
        return value == ITimestampConstantValue.MIN_VALUE;
    }

    /** Checks if the timestamp is the invalid timestamp.
     *
     * @return          True is the timestamp is an invalid timestamp, false otherwise.
     */
    public boolean isInvalid()
    {
        return value == ITimestampConstantValue.INVALID_VALUE;
    }

    /** Gets the raw value of the timestamp.
     *
     * @return          A ITimestampConstantValue
     */
    ITimestampConstantValue getRaw()
    {
        return value;
    }

    /** Get the hashcode
     *
     *  @return         An int representing the hashcode for this instance.
     */
    @Override
    public int hashCode()
    {
        return getRaw().ordinal();
    }

    /** Check for equality
     *
     *  @return         An bool indicating whether two objects are equal.
     */
    @Override
    public boolean equals(Object o)
    {
        if (o instanceof ITimestampConstant)
        {
            ITimestampConstant c = (ITimestampConstant) o;
            if (c.getRaw() == this.getRaw()) { return true; }
        }
        return false;
    }

    /**
     * Return the string representation of this timestamp.
     *
     * @return          A timestamp string
     */
    @Override
    public String toString()
    {
        return getRaw().toString();
    }

}

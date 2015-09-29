package org.corfudb.runtime.stream;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

/**
 * A logical async timestamp is used to timestamp async batches.
 *
 * We treat the batch number as the most significant number.
 * (batches are retrieved from montonically incrementing sections of the log).
 *
 * Within a batch, the acquisition number is used for comparison.
 * Created by mwei on 9/28/15.
 */
@Data
@EqualsAndHashCode(exclude="batchCount")
@Slf4j
public class LogicalAsyncTimestamp implements ITimestamp {

    /** The batch of this timestamp. */
    final long batchNumber;

    /** The acquisition of this timestamp. */
    final long acquisitionNumber;

    /** The number of timestamps in this batch */
    final long batchCount;

    /** Get the string representation of this logical async timestamp. */
    @Override
    public String toString()
    {
        return Long.toString(batchNumber) + "." + Long.toString(acquisitionNumber);
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

        //We compare batch order first, and use that unless it is 0, in which case acquisitionNumber is used at tie-breaker.
        if (timestamp instanceof LogicalAsyncTimestamp)
        {
            int batchCompare = Long.compare(this.batchNumber, ((LogicalAsyncTimestamp) timestamp).batchNumber);
            return batchCompare != 0 ? batchCompare :
                    Long.compare(this.acquisitionNumber, ((LogicalAsyncTimestamp) timestamp).acquisitionNumber);
        }
        throw new ClassCastException("Unknown timestamp!");
    }

    public static ITimestamp getNextTimestamp(ITimestamp ts)
    {
        if (ITimestamp.isMin(ts))
        {
            return new LogicalAsyncTimestamp(0, 0, Long.MAX_VALUE);
        }
        else if (!(ts instanceof LogicalAsyncTimestamp))
        {
            throw new ClassCastException("Incorrect timestamp class");
        }
        else
        {
            LogicalAsyncTimestamp t = (LogicalAsyncTimestamp)ts;
            if (t.acquisitionNumber + 1 >= t.batchCount)
                return new LogicalAsyncTimestamp(t.batchNumber+1, 0L , Long.MAX_VALUE);
            return new LogicalAsyncTimestamp(t.batchNumber, t.acquisitionNumber+1, t.batchCount);
        }
    }

    public static ITimestamp getPreviousTimestamp(ITimestamp ts)
    {
        if (!(ts instanceof LogicalAsyncTimestamp))
        {
            throw new ClassCastException("Incorrect timestamp class");
        }
        else
        {
            LogicalAsyncTimestamp t = (LogicalAsyncTimestamp)ts;
            if (t.acquisitionNumber == 0)
                return new LogicalAsyncTimestamp(t.batchNumber-1, Long.MAX_VALUE , Long.MAX_VALUE);
            return new LogicalAsyncTimestamp(t.batchNumber, t.acquisitionNumber-1, t.batchCount);
        }
    }
}

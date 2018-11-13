package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * A Logical Sequence Number (LSN) adds the notion of time (i.e., epoch) to a sequence number. This
 * tuple
 *
 * A sequence number on its own represents a slot in the space of log addresses, but this
 * does not necessarily capture the notion of time. The main reason lies on the fact that our
 * system might observe regressions in the address space whenever there is a sequencer failover
 * and non-materialized log entries are at the tail of the log
 * (i.e., issued tokens that have not been written to). For this reason a logical sequence number is the
 * correlation of a sequence number and the epoch in which this was issued.
 *
 */
/**
 * LogicalSequenceNumber returned by the sequencer is a combination of the
 * sequence number and the epoch at which it was acquired.
 *
 * <p>Created by zlokhandwala on 4/7/17.</p>
 */
@Data
@AllArgsConstructor
public class LogicalSequenceNumber implements Comparable<LogicalSequenceNumber>,
        ICorfuPayload<LogicalSequenceNumber> {

    /** The epoch corresponding to this timestamp, i.e., a stamp of the system's logical time
     * when this timestamp was issued. */
    private final long epoch;

    /** The global address/snapshot corresponding to this timestamp. */
    private final long sequenceNumber;

    public LogicalSequenceNumber(ByteBuf buf) {
        epoch = ICorfuPayload.fromBuffer(buf, Long.class);
        sequenceNumber = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, epoch);
        ICorfuPayload.serialize(buf, sequenceNumber);
    }

    @Override
    public String toString() {
        return String.format("({},{})", epoch, sequenceNumber);
    }

    @Override
    public int compareTo(LogicalSequenceNumber other) {
        int epochCompare = Long.compare(this.epoch, other.epoch);
        if (epochCompare == 0) {
            return Long.compare(this.sequenceNumber, other.getSequenceNumber());
        }
        return epochCompare;
    }

    /**
     * Gets a default LogicalSequenceNumber.
     */
    public static LogicalSequenceNumber getDefaultLSN() {
        return new LogicalSequenceNumber(-1L, -1L);
    }

    public static LogicalSequenceNumber decode(String lsn) {
        Long epoch = -1L;
        Long sequenceNumber = -1L;

        Pattern pattern = Pattern.compile("\\((.*?)\\)");
        Matcher matcher = pattern.matcher(lsn);
        if (matcher.find()) {
            String[] epochSequencer = matcher.group(1).split(",");
            epoch = Long.decode(epochSequencer[0]);
            sequenceNumber = Long.decode(epochSequencer[1]);
        }

        return new LogicalSequenceNumber(epoch, sequenceNumber);
    }

    /**
     * Get Max Logical Sequence Number for a given pair
     * @param o
     * @return maximum LSN
     */
    public LogicalSequenceNumber getMax(@Nonnull LogicalSequenceNumber o) {
        return isEqualOrGreaterThan(o) ? this : o;
    }

    public boolean isGreaterThan(LogicalSequenceNumber other) {
        return this.compareTo(other) > 0;
    }

    public boolean isEqualOrGreaterThan(LogicalSequenceNumber other) {
        return this.compareTo(other) >= 0;
    }

    public boolean isLessThan(LogicalSequenceNumber other) {
        return this.compareTo(other) < 0;
    }

    public boolean isEqualOrLessThan(LogicalSequenceNumber other) {
        return this.compareTo(other) <= 0;
    }

    public boolean isEqualTo(LogicalSequenceNumber other) {
        return this.compareTo(other) == 0;
    }

    public LogicalSequenceNumber getPrevious() {
        return increment(-1L);
    }

    public LogicalSequenceNumber increment(long delta) {
        return new LogicalSequenceNumber(this.epoch, this.sequenceNumber + delta);
    }
}

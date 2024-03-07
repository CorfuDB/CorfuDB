package org.corfudb.runtime.collections.streaming;

import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.BufferUtils;
import org.apache.commons.collections.buffer.BoundedFifoBuffer;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.ReadOptions;
import org.corfudb.runtime.view.stream.StreamAddressSpace;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unlike the {@link org.corfudb.runtime.view.stream.IStreamView} this stream implementation only tracks updates
 * that haven't been checkpointed.
 */
@Slf4j
public class DeltaStream {

    private final ReadOptions options = ReadOptions
            .builder()
            .clientCacheable(false)
            .ignoreTrim(false)
            .waitForHole(true)
            .serverCacheable(false)
            .build();

    private final AddressSpaceView addressSpaceView;

    /**
     * Stream tag id
     */
    @Getter
    private final UUID streamId;

    /**
     * Last Address consumed/read by this stream
     */
    private final AtomicLong lastAddressRead;

    /**
     * Max seen address for this stream from the last refresh/sync
     */
    private final AtomicLong maxAddressSeen;

    /**
     * The last trim mark that has been observed
     */
    private final AtomicLong trimMark;

    /**
     * The size of this stream's buffer (i.e., pending addresses to read)
     */
    private final int bufferSize;

    /**
     * A circular buffer that holds the queued address to read
     */
    private final Buffer addressesToRead;

    @Data
    @AllArgsConstructor
    private static class TimeStampedRead {
        private final long address;
        private final long timestamp;
    }

    public DeltaStream(AddressSpaceView addressSpaceView, UUID streamId, long lastAddressRead, int bufferSize) {
        this.addressSpaceView = addressSpaceView;
        Preconditions.checkArgument(Address.NON_ADDRESS == -1);
        Preconditions.checkArgument(lastAddressRead >= Address.NON_ADDRESS,
                "lastAddressRead %s must be >= %s", lastAddressRead, Address.NON_ADDRESS);
        this.streamId = streamId;
        this.bufferSize = bufferSize;
        this.addressesToRead = BufferUtils.synchronizedBuffer(new BoundedFifoBuffer(this.bufferSize));
        this.lastAddressRead = new AtomicLong(lastAddressRead);
        this.maxAddressSeen = new AtomicLong(lastAddressRead);
        this.trimMark = new AtomicLong(Address.NON_ADDRESS);
    }

    public long getMaxAddressSeen() {
        return maxAddressSeen.get();
    }

    /**
     * Check if the last address read was trimmed, since the last address has to always be
     * greater than the trim mark, if this condition is true, then a trim has created a gap
     * that hasn't been synced and cannot be recovered.
     */
    private boolean trimGap() {
        return Address.isAddress(trimMark.get())
                && trimMark.get() != Address.NON_ADDRESS && lastAddressRead.get() < trimMark.get();
    }

    /**
     * This method is called by the scheduler to check if it can queue more addresses to to
     * this stream.
     * @return available buffer space
     */
    public int availableSpace() {
        int availableSpace = this.bufferSize - addressesToRead.size();
        Preconditions.checkState(availableSpace >= 0);
        return availableSpace;
    }

    private TimeStampedRead stampRead(long readAddress) {
        return new TimeStampedRead(readAddress, System.nanoTime());
    }

    /**
     * When the scheduler decides to poll a stream it can discover new deltas for the stream.
     * The deltas - addresses to be read - are transferred from the scheduler to the DeltaStream via
     * this method. It's possible that the stream cannot accept all the discovered addresses because it
     * doesn't have enough buffer space, in that case it will try to fill up the buffer and discard the
     * rest of the addresses. The discarded addresses will be rediscovered on the subsequent poll. This
     * behavior is required to keep a bound on memory usage and not have queues that grow indefinitely (in case of
     * a slow worker/consumer).
     *
     * Note: if a trim has been detected this method will short-circuit. In other words, only the trim mark
     * will be update and the transfer of newly discovered addresses won't happen. The trim mark is updated such
     * that the subsequent call to hasNext() and next() will produce a TrimmedException.
     * @param streamAddressSpace New addresses discovered from (maxAddressSeen, inf+)
     */
    public void refresh(StreamAddressSpace streamAddressSpace) {
        if (log.isTraceEnabled()) {
            log.trace("refresh id {} max {} trimMark {} newTrim {} size {} newAddresses {}", streamId, maxAddressSeen,
                    trimMark, streamAddressSpace.getTrimMark(), streamAddressSpace.toArray().length,
                    streamAddressSpace.toArray());
        }

        if (lastAddressRead.get() < streamAddressSpace.getTrimMark()
                || maxAddressSeen.get() < streamAddressSpace.getTrimMark()) {

            // Since a trim has been detected, there is no need to transfer the addresses, we just
            // need to update the trim mark, on the subsequent sync, the task will error out before attempting
            // to read, notice that a trim exception can occur even if StreamAddressSpace is empty
            trimMark.set(streamAddressSpace.getTrimMark());

            // if the task is syncing it will either discover the trim during reading the addresses that
            // are already buffered, or discover this trim the next time the task gets scheduled

            // since refresh can be called while the task is syncing we can error out when the scheduling tries to
            // reschedule this task
            return;
        }

        if (Address.isAddress(streamAddressSpace.getTrimMark())) {
            if (streamAddressSpace.getTrimMark() < trimMark.get()) {
                // As per Corfu's current design, this check can only be an error message (instead of an actual
                // validation leading to an exception).
                // Current 'prefix trim' protocol allows the system to be in a state such that the start address of all
                // nodes is not the same, i.e., whenever prefixTrim has not gone through all LUs (for instance
                // consider the case where a TimeoutException hits on the last LU during prefixTrim). If this
                // LU becomes the new sequencer during this time, its trim mark is expected to regress wrt the other
                // LUs.
                // For this error message to become an actual validation it needs to be backed-up by the protocol.
                log.warn("StreamId={} new trimMark {} should be >= {}, unless trim mark regressed due to " +
                                "NEW sequencer bootstrapped from untrimmed log unit.", streamId,
                        streamAddressSpace.getTrimMark(), trimMark);
            }

            // Adopt new trim mark
            trimMark.set(streamAddressSpace.getTrimMark());
        }

        int maxAddressesToTransfer = availableSpace();
        long[] newAddresses = streamAddressSpace.toArray();
        for (int idx = 0; idx < newAddresses.length && idx < maxAddressesToTransfer; idx++) {
            // if maxAddressesToTransfer doesn't fit, then the rest of the addresses will be
            // thrown away and re-discovered on the next poll
            // Note that this predicate can fail in certain edge cases where the sequencer
            // can regress. This is an existing issue in the current streaming layer and this
            // PR doesn't attempt to fix the problem, but rather fail explicitly instead of
            // ignoring the fault.
            Preconditions.checkState(maxAddressSeen.get() < newAddresses[idx],
                    "maxAddressSeen %s not < %s", maxAddressSeen, newAddresses[idx]);
            Preconditions.checkState(addressesToRead.add(stampRead(newAddresses[idx])),
                    "failed to queue %d", newAddresses[idx]);
            // Only update max seen after it has been added to the buffer
            maxAddressSeen.set(newAddresses[idx]);
            log.trace("refresh: transferring {} id {}", newAddresses, streamId);
        }
    }

    /**
     * Has next indicates whether there is more data to read, or if there has been a trimmed exception detected.
     * @return return true if a there are pending reads, or if a trimmed exception occurred, otherwise return false
     */
    public boolean hasNext() {
        return !addressesToRead.isEmpty() || trimGap();
    }

    public ILogData next() {

        if (lastAddressRead.get() < trimMark.get()) {
            throw new TrimmedException(String.format("lastAddressRead %s trimMark %s", lastAddressRead, trimMark));
        }

        TimeStampedRead stampedRead = (TimeStampedRead) addressesToRead.remove();
        long readAddress = stampedRead.getAddress();

        Preconditions.checkState(lastAddressRead.get() < readAddress,
                "%d must be greater than %d", readAddress, lastAddressRead);

        if (readAddress <= trimMark.get()) {
            throw new TrimmedException(String.format("current %d trimMark %s", readAddress, trimMark));
        }

        long startRead = System.nanoTime();
        ILogData logData = addressSpaceView.read(readAddress, options);
        MicroMeterUtils.time(Duration.ofNanos(System.nanoTime() - startRead),"delta_stream.read");

        if (!logData.isHole()) {
            // if its not a hole then it must belong to our stream
            Preconditions.checkState(logData.hasBackpointer(streamId), "%s must contain %s",
                    logData.getBackpointerMap().keySet(), streamId);
        }

        Preconditions.checkState(logData.getGlobalAddress().equals(readAddress));
        // The delta stream shouldn't see any checkpoint entries
        Preconditions.checkState(!logData.hasCheckpointMetadata(),
                "Address %s has checkpoint data", logData.getGlobalAddress());
        lastAddressRead.set(readAddress);

        // Since this method is called by the consumer the queuing delay reflects the time
        // the time difference between when the address was read and when the address was discovered
        MicroMeterUtils.time(Duration.ofNanos(System.nanoTime() - stampedRead.getTimestamp()),
                "delta_stream.queuing_delay", "streamId", streamId.toString());
        return logData;
    }
}

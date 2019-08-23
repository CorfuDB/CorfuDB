package org.corfudb.runtime.object;

import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.corfudb.protocols.logprotocol.SMRRecord;
import org.corfudb.protocols.logprotocol.SMRRecordLocator;
import org.corfudb.protocols.wireprotocol.TokenResponse;

/**
 * On top of a stream, an SMR object layer implements objects whose history of updates are backed
 * by a stream.
 *
 * <p>This class defines an API for supporting stream operations on the SMR layer.
 * ISMRStream wraps a pure stream and provides a similar API: append, remainingTo, current,
 * previous, pos and seek.
 * Different from a stream, the entries returned from methods that obtain stream entries,
 * like current, previous, are of type SMRRecord.
 *
 * <p>Created by mwei on 3/13/17.
 */
@SuppressWarnings("checkstyle:abbreviation")
public interface ISMRStream {


    List<SMRRecord> remainingUpTo(long maxGlobal);

    List<SMRRecord> current();

    List<SMRRecord> previous();

    long pos();

    void reset();

    void seek(long globalAddress);

    void gc(long trimMark);

    Stream<SMRRecord> stream();

    Stream<SMRRecord> streamUpTo(long maxGlobal);

    /**
     * Append a SMRRecord to the stream, returning the global address
     * it was written at.
     *
     * <p>Optionally, provide a method to be called when an address is acquired,
     * and also a method to be called when an address is released (due to
     * an unsuccessful append).
     * </p>
     *
     * @param entry                 The SMR entry to append.
     * @param acquisitionCallback   A function to call when an address is
     *                              acquired.
     *                              It should return true to continue with the
     *                              append.
     * @param deacquisitionCallback A function to call when an address is
     *                              released. It should return true to retry
     *                              writing.
     * @return The (global) address the object was written at.
     */
    long append(SMRRecord entry,
                Function<TokenResponse, Boolean> acquisitionCallback,
                Function<TokenResponse, Boolean> deacquisitionCallback);

    /**
     * Get the UUID for this stream (optional operation).
     *
     * @return The UUID for this stream.
     */
    @SuppressWarnings("checkstyle:abbreviation")
    default UUID getID() {
        return new UUID(0L, 0L);
    }

    static void addLocatorToSMRRecords(List<SMRRecord> smrRecords, long globalAddress, UUID streamId) {
        IntStream.range(0, smrRecords.size()).forEach(i -> {
            SMRRecord entry = smrRecords.get(i);
            // It is not necessary to compute locator when it has been computed
            if (entry.locator == null) {
                entry.setLocator(new SMRRecordLocator(
                        globalAddress,
                        streamId,
                        i,
                        entry.getSerializedSize()));
            }
        });
    }
}

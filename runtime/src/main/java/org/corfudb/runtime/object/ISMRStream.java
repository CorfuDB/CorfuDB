package org.corfudb.runtime.object;

import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.TokenResponse;

/**
 * On top of a stream, an SMR object layer implements objects whose history of updates are backed
 * by a stream.
 *
 * <p>This class defines an API for supporting stream operations on the SMR layer.
 * ISMRStream wraps a pure stream and provides a similar API: append, remainingTo, current,
 * previous, pos and seek.
 * Different from a stream, the entries returned from methods that obtain stream entries,
 * like current, previous, are of type SMREntry.
 *
 * <p>Created by mwei on 3/13/17.
 */
@Deprecated // TODO: Add replacement method that conforms to style
@SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
public interface ISMRStream {


    List<SMREntry> remainingUpTo(long maxGlobal);

    List<SMREntry> current();

    List<SMREntry> previous();

    long pos();

    void reset();

    void seek(long globalAddress);

    Stream<SMREntry> stream();

    Stream<SMREntry> streamUpTo(long maxGlobal);

    /**
     * Append a SMREntry to the stream, returning the global address
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
    long append(SMREntry entry,
                Function<TokenResponse, Boolean> acquisitionCallback,
                Function<TokenResponse, Boolean> deacquisitionCallback);

    /**
     * Get the UUID for this stream (optional operation).
     *
     * @return The UUID for this stream.
     */
    @Deprecated // TODO: Add replacement method that conforms to style
    @SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
    default UUID getID() {
        return new UUID(0L, 0L);
    }

//    public boolean isSnapshotAvailable(long timestamp);
}

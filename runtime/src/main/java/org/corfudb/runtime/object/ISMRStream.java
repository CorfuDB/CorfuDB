package org.corfudb.runtime.object;

import org.corfudb.protocols.logprotocol.ISMRConsumable;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.view.Address;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by mwei on 3/13/17.
 */
public interface ISMRStream {


    List<SMREntry> remainingUpTo(long maxGlobal);

    List<SMREntry> current();

    List<SMREntry> previous();

    long pos();

    void reset();

    void seek(long globalAddress);

    /** Append a SMREntry to the stream, returning the global address
     * it was written at.
     * <p>
     * Optionally, provide a method to be called when an address is acquired,
     * and also a method to be called when an address is released (due to
     * an unsuccessful append).
     * </p>
     * @param   entry               The SMR entry to append.
     * @param   acquisitionCallback A function to call when an address is
     *                              acquired.
     *                              It should return true to continue with the
     *                              append.
     * @param   deacquisitionCallback A function to call when an address is
     *                                released. It should return true to retry
     *                                writing.
     * @return  The (global) address the object was written at.
     */
    long append(SMREntry entry,
                Function<TokenResponse, Boolean> acquisitionCallback,
                Function<TokenResponse, Boolean> deacquisitionCallback);
}

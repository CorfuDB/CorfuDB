package org.corfudb.runtime.view.replication;

import java.util.function.Function;
import javax.annotation.Nonnull;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.exceptions.HoleFillRequiredException;

/**
 * Created by mwei on 4/6/17.
 */
public interface IHoleFillPolicy {

    /** Apply the given peek function until hole filling is required or
     * committed data is returned. If hole filling is required a
     * HoleFillRequiredException is thrown.
     *
     * @param address                   The address to apply the function.
     *
     * @param peekFunction              The function to use to peek data
     *                                  from the log.
     *
     * @return                          The committed data at the given address.
     * @throws HoleFillRequiredException  If hole filling is required.
     */
    @Nonnull
    ILogData peekUntilHoleFillRequired(long address,
                                       Function<Long, ILogData> peekFunction)
            throws HoleFillRequiredException;
}

package org.corfudb.runtime.view.replication;

import java.util.function.Function;
import javax.annotation.Nonnull;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.exceptions.HoleFillRequiredException;

/** A simple hole filling policy which aggressively
 * fills holes whenever there is a failed read.
 *
 * <p>Created by mwei on 4/6/17.
 */
public class AlwaysHoleFillPolicy implements IHoleFillPolicy {

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public ILogData peekUntilHoleFillRequired(long address,
               Function<Long, ILogData> peekFunction) throws HoleFillRequiredException {
        ILogData data = peekFunction.apply(address);
        if (data == null) {
            throw new HoleFillRequiredException("No data at address");
        }
        return data;
    }
}

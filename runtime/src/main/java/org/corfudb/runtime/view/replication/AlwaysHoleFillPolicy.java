package org.corfudb.runtime.view.replication;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.exceptions.HoleFillRequiredException;

import javax.annotation.Nonnull;
import java.util.function.Function;

/** A simple hole filling policy which aggressively
 * fills holes whenever there is a failed read.
 *
 * Created by mwei on 4/6/17.
 */
public class AlwaysHoleFillPolicy implements IHoleFillPolicy {

    /** {@inheritDoc} */
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

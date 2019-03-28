package org.corfudb.runtime.view.replication;

import java.util.function.Function;
import javax.annotation.Nonnull;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.exceptions.HoleFillRequiredException;
import lombok.extern.slf4j.Slf4j;

/** A simple hole filling policy which aggressively
 * fills holes whenever there is a failed read.
 *
 * <p>Created by mwei on 4/6/17.
 */
@Slf4j
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
            log.info("Data is null");
            throw new HoleFillRequiredException("No data at address");
        }
        log.info("Data:{} type:{} isEmpty:{}", data, data.getType(), data.isEmpty());
        return data;
    }
}

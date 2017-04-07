package org.corfudb.runtime.view.replication;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.exceptions.HoleFillPolicyException;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * This hole fill policy keeps retrying and never requires
 * a hole fill. It waits a static amount of time before
 * retrying.
 *
 * Created by mwei on 4/6/17.
 */
@Slf4j
public class NeverHoleFillPolicy implements IHoleFillPolicy {

    /** The amount of time to wait before retries. */
    int waitMs;

    /** {@inheritDoc} */
    @Nonnull
    @Override
    public ILogData peekUntilHoleFillRequired(long address,
            Function<Long, ILogData> peekFunction) throws HoleFillPolicyException {
        ILogData data = null;
        int tryNum = 0;
        do {
            if (tryNum != 0) {
                try {
                    log.trace("Peek[{}] Retrying read", tryNum);
                    Thread.sleep(waitMs);
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
            }
            data = peekFunction.apply(address);
        } while (data == null);
        return data;
    }
}

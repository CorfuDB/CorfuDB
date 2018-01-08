package org.corfudb.runtime.view.replication;

import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.exceptions.HoleFillRequiredException;
import org.corfudb.util.Sleep;


/**
 * This hole fill policy keeps retrying and never requires
 * a hole fill. It waits a static amount of time before
 * retrying.
 *
 * <p>Created by mwei on 4/6/17.
 */
@Slf4j
public class NeverHoleFillPolicy implements IHoleFillPolicy {

    /** The amount of time to wait before retries. */
    final int waitMs;

    /** Create a new neverHoleFillPolicy with the given wait time.
     *
     * @param waitMs    The time to wait, in milliseconds.
     */
    public NeverHoleFillPolicy(int waitMs) {
        this.waitMs = waitMs;
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public ILogData peekUntilHoleFillRequired(long address,
            Function<Long, ILogData> peekFunction) throws HoleFillRequiredException {
        ILogData data = null;
        int tryNum = 0;
        do {
            if (tryNum != 0) {
                log.trace("Peek[{}] Retrying read {}", address, tryNum);
                Sleep.MILLISECONDS.sleepUninterruptibly(waitMs);
            }
            data = peekFunction.apply(address);
            tryNum++;
        } while (data == null);
        return data;
    }
}

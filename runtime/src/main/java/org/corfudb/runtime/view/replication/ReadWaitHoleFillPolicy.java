package org.corfudb.runtime.view.replication;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import javax.annotation.Nonnull;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.exceptions.HoleFillRequiredException;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.RetryNeededException;
import lombok.extern.slf4j.Slf4j;


/**
 * A hole filling policy which reads several times,
 * waiting a static amount of time in between, before
 * requiring a hole fill.
 *
 * <p>Created by mwei on 4/6/17.
 */
@Slf4j
public class ReadWaitHoleFillPolicy implements IHoleFillPolicy {

    /**
     * Duration after which no more read attempts are made and an address hole fill is
     * attempted.
     */
    private final Duration holeFillThreshold;

    /**
     * Wait interval between consecutive read attempts to cap exponential back-off.
     */
    private final Duration retryWaitThreshold;

    /**
     * Create a ReadWaitHoleFillPolicy with the given wait times
     * and retries.
     *
     * @param holeFillThreshold  The amount of time to wait before retrying.
     * @param retryWaitThreshold The number of retries to apply before requiring a hole fill.
     */
    public ReadWaitHoleFillPolicy(Duration holeFillThreshold, Duration retryWaitThreshold) {
        this.holeFillThreshold = holeFillThreshold;
        this.retryWaitThreshold = retryWaitThreshold;
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public ILogData peekUntilHoleFillRequired(long address, Function<Long, ILogData> peekFunction)
            throws HoleFillRequiredException {
        final AtomicLong startTime = new AtomicLong();

        try {
            return IRetry.build(ExponentialBackoffRetry.class, RetryExhaustedException.class, () -> {

                // Try the read
                ILogData data = peekFunction.apply(address);
                // If it was not null, we can return it.
                if (data != null) {
                    return data;
                } else if (startTime.get() == 0) {
                    startTime.set(System.currentTimeMillis());
                } else if (System.currentTimeMillis() - startTime.get() >= holeFillThreshold
                        .toMillis()) {
                    throw new RetryExhaustedException("Retries Exhausted.");
                }

                // Otherwise try again.
                log.trace("peekUntilHoleFillRequired: Attempted read at address {}, "
                            + "but data absent. Retrying.", address);
                throw new RetryNeededException();
            }).setOptions(x -> x.setMaxRetryThreshold(retryWaitThreshold)).run();
        } catch (InterruptedException ie) {
            throw new UnrecoverableCorfuInterruptedError(ie);
        } catch (RetryExhaustedException ree) {
            // Retries exhausted. Hole filling.
            log.debug("peekUntilHoleFillRequired: Address:{} empty. Hole-filling.", address);
        }

        throw new HoleFillRequiredException("No data after " + holeFillThreshold.toMillis() + "ms.");
    }
}

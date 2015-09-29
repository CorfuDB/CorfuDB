package org.corfudb.runtime.smr.HoleFillingPolicy;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.HoleEncounteredException;
import org.corfudb.runtime.stream.IStream;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.TimeUnit;

/**
 * A simple hole-filling policy based on timeouts.
 */
@Slf4j
public class TimeoutHoleFillPolicy implements IHoleFillingPolicy {

    HoleEncounteredException lastException;
    LocalDateTime lastExceptionTime;
    long time;
    TemporalUnit unit;

    /**
     * Create a hole filling policy with a default timeout of 30ms.
     */
    public TimeoutHoleFillPolicy()
    {
        this(30, ChronoUnit.MILLIS);
    }

    /**
     * Create a hole filling policy
     * @param time
     * @param tu
     */
    public TimeoutHoleFillPolicy(long time, @NonNull TemporalUnit tu)
    {
        this.time = time;
        this.unit = tu;
    }


    @Override
    public boolean apply(HoleEncounteredException he, IStream s) {
        if (lastException == null || !lastException.getAddress().equals(he.getAddress())) {
            lastException = he;
            lastExceptionTime = LocalDateTime.now();
            return false;
        }

        if (lastExceptionTime.compareTo(LocalDateTime.now().minus(time, unit)) < 0)
        {
            log.warn("Stream {} exceeded hole filling timeout, filling hole @ {}", s.getStreamID(), he.getAddress());
            return s.fillHole(he.getAddress());
        }

        return false;
    }
}

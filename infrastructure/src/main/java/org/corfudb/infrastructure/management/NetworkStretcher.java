package org.corfudb.infrastructure.management;

import com.google.common.annotations.VisibleForTesting;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;

import java.time.Duration;

/**
 * NetworkStretcher increases or decreases the timeout intervals for polling based on
 * whether the poll was unsuccessful or successful respectively.
 */
@Builder
public class NetworkStretcher {
    /**
     * Max duration for the responseTimeouts of the routers.
     * In the worst case scenario or in case of failed servers, their response timeouts will be
     * set to a maximum value of maxPeriod.
     */
    @Getter
    @Default
    private final Duration maxPeriod = Duration.ofSeconds(5);

    @Getter
    @Default
    private final Duration minPeriod = Duration.ofSeconds(2);

    /**
     * Response timeout for every router.
     */
    @Getter
    @Default
    private Duration currentPeriod = Duration.ofSeconds(2);

    /**
     * Poll interval between iterations in a pollRound
     */
    @Default
    private final Duration initialPollInterval = Duration.ofSeconds(1);

    /**
     * Increments in which the period moves towards the maxPeriod in every failed
     * iteration provided.
     */
    @Default
    private final Duration periodDelta = Duration.ofSeconds(1);

    /**
     * Function to increment the existing response timeout period.
     *
     * @return The new calculated timeout value.
     */
    @VisibleForTesting
    Duration getIncreasedPeriod() {
        Duration increasedPeriod = currentPeriod.plus(periodDelta);
        if (increasedPeriod.toMillis() > maxPeriod.toMillis()){
            return maxPeriod;
        }

        return increasedPeriod;
    }

    /**
     * Function to decrement the existing response timeout period.
     *
     * @return The new calculated timeout value.
     */
    private Duration getDecreasedPeriod() {
        Duration decreasedPeriod = currentPeriod.minus(periodDelta);

        if (decreasedPeriod.toMillis() < minPeriod.toMillis()) {
            return minPeriod;
        }

        return decreasedPeriod;
    }

    /**
     * Tune timeouts after each poll iteration, according to list of failed and connected nodes
     *
     */
    public void modifyIterationTimeouts() {
        currentPeriod = getIncreasedPeriod();
    }

    /**
     * Calculates rest interval according to the time already spent on network calls
     * @param elapsedTime time already spent by making network calls
     * @return rest interval
     */
    public Duration getRestInterval(Duration elapsedTime){

        Duration restInterval = currentPeriod.minus(elapsedTime);
        if (restInterval.toMillis() < initialPollInterval.toMillis()){
            restInterval = initialPollInterval;
        }
        return restInterval;
    }

    public void modifyDecreasedPeriod() {
        currentPeriod = getDecreasedPeriod();
    }
}

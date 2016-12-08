package org.corfudb;

import java.time.Duration;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.SECONDS;


/** This class contains automatically calculated parameters used for timeouts
 * and concurrency throughout the infrastructure.
 * Created by mwei on 12/8/16.
 */
public class CorfuTestParameters {
    /** A very short timeout, typically in the order of a 100ms.
     * You might expect a simple request to timeout in this timeframe.
     */
    public final Duration TIMEOUT_VERY_SHORT = Duration.of(100, MILLIS);

    /** A short timeout, typically in the order of 1s.
     * You might expect a typical request to timeout in this timeframe.
     */
    public final Duration TIMEOUT_SHORT = Duration.of(1, SECONDS);

    /** A normal timeout, typical in the order of 10s.
     * Your might expect a request like an IO flush to timeout in
     * this timeframe.
     */
    public final Duration TIMEOUT_NORMAL = Duration.of(10, SECONDS);

    /** A long timeout, typically in the order of 1m.
     * You would expect an entire unit test to timeout in this timeframe.
     */
    public final Duration TIMEOUT_LONG = Duration.of(1, MINUTES);
}

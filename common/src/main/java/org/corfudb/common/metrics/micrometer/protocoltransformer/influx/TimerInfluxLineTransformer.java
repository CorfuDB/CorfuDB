package org.corfudb.common.metrics.micrometer.protocoltransformer.influx;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Matcher transformer that matches the lines produced by the timer metric
 * and transforms them into the InfluxDb line protocol.
 */
public class TimerInfluxLineTransformer implements InfluxLineProtocolTransformer {
    protected static final String TIMER_PATTERN_STRING =
            METRIC_PATTERN_STRING + " (throughput=[\\d]+/s mean=[\\d.]+s max=[\\d.]+s)$";
    private final Pattern timerPattern = Pattern.compile(TIMER_PATTERN_STRING);
    private Optional<Matcher> matched = Optional.empty();

    @Override
    public Optional<Matcher> getMatched() {
        return matched;
    }

    @Override
    public boolean test(String s) {
        Matcher matcher = timerPattern.matcher(s);
        this.matched = Optional.of(matcher);
        return matcher.find();
    }
}

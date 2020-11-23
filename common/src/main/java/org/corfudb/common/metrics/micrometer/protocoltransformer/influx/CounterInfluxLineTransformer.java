package org.corfudb.common.metrics.micrometer.protocoltransformer.influx;

import org.corfudb.common.metrics.micrometer.protocoltransformer.PatternMatcher;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Matcher transformer that matches the lines produced by the counter metric
 * and transforms them into the InfluxDb line protocol.
 */
public class CounterInfluxLineTransformer implements InfluxLineProtocolTransformer {
    protected static final String COUNTER_PATTERN_STRING = PatternMatcher.METRIC_PATTERN_STRING + " (throughput=[\\d.]+/s)$";
    private final Pattern counterPattern = Pattern.compile(COUNTER_PATTERN_STRING);
    private Optional<Matcher> matched = Optional.empty();

    @Override
    public Optional<Matcher> getMatched() {
        return matched;
    }

    @Override
    public boolean test(String s) {
        Matcher matcher = counterPattern.matcher(s);
        this.matched = Optional.of(matcher);
        return matcher.matches();
    }
}

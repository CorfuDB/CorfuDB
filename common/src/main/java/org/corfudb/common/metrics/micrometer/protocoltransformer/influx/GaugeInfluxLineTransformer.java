package org.corfudb.common.metrics.micrometer.protocoltransformer.influx;

import org.corfudb.common.metrics.micrometer.protocoltransformer.PatternMatcher;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Matcher transformer that matches the lines produced by the gauge metric
 * and transforms them into the InfluxDb line protocol.
 */
public class GaugeInfluxLineTransformer implements InfluxLineProtocolTransformer {
    static final String GAUGE_PATTERN_STRING = PatternMatcher.METRIC_PATTERN_STRING + " (value=[\\d]+$)";
    private final Pattern gaugePattern = Pattern.compile(GAUGE_PATTERN_STRING);
    private Optional<Matcher> matched = Optional.empty();

    @Override
    public Optional<Matcher> getMatched() {
        return matched;
    }

    @Override
    public boolean test(String s) {
        Matcher matcher = gaugePattern.matcher(s);
        this.matched = Optional.of(matcher);
        return matcher.find();
    }
}

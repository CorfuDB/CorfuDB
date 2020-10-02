package org.corfudb.common.metrics.micrometer.protocoltransformer.influx;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Matcher transformer that matches the lines produced by the long running task metric
 * and transforms them into the InfluxDb line protocol.
 */
public class LongRunningTaskInfluxLineTransformer implements InfluxLineProtocolTransformer {
    static final String LONG_RUNNING_TASK_PATTERN_STRING =
            METRIC_PATTERN_STRING + " (active=1 [a-z]+ duration=[\\d.]+s)$";
    private final Pattern longRunningTaskPattern = Pattern.compile(LONG_RUNNING_TASK_PATTERN_STRING);
    private Optional<Matcher> matched = Optional.empty();

    @Override
    public Optional<Matcher> getMatched() {
        return matched;
    }

    @Override
    public boolean test(String s) {
        Matcher matcher = longRunningTaskPattern.matcher(s);
        this.matched = Optional.of(matcher);
        return matcher.find();
    }
}

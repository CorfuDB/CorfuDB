package org.corfudb.common.metrics.micrometer.protocoltransformer;

import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Matcher;

/**
 * An interface that tests the line and returns the matched result.
 */
public interface PatternMatcher extends Predicate<String> {
    /**
     * A pattern string that contains the name of the metric as well as
     * the previously defined tags.
     */
    String METRIC_PATTERN_STRING = "^([a-z.]+)\\{([a-z0-9.:-=,]+)}";

    /**
     * Return the optionally matched result.
     * @return An optionally matched result
     */
    Optional<Matcher> getMatched();
}

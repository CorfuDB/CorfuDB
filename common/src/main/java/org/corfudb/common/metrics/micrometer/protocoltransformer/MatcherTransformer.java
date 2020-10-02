package org.corfudb.common.metrics.micrometer.protocoltransformer;

import java.util.Optional;

/**
 * An interface that matches and transforms the line if match is successful.
 */
public interface MatcherTransformer extends PatternMatcher, ProtocolTransformer  {

    /**
     * If the line matches the predicate, transform it.
     * @param line A provided line.
     * @return An optional transformed result.
     */
    default Optional<String> transformIfMatches(String line) {
        if (test(line)) {
            return getMatched()
                    .flatMap(m -> transform(m.group()));
        }
        return Optional.empty();
    }

}

package org.corfudb.common.metrics.micrometer.protocoltransformer;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/**
 * Matches and transforms the line with one of the provided matcher transformers.
 */
@AllArgsConstructor
@Slf4j
public class LineTransformer {
    @NonNull
    private final ImmutableList<MatcherTransformer> matcherTransformers;

    public synchronized Optional<String> transformLine(String line) {
        for (MatcherTransformer mt: matcherTransformers) {
            Optional<String> transformedString = mt.transformIfMatches(line);
            if (transformedString.isPresent()) {
                return transformedString;
            }
        }
        log.trace("Failed to parse: {}", line);
        return Optional.empty();
    }
}

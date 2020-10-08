package org.corfudb.common.metrics.micrometer.protocoltransformer;


import java.util.Optional;

/**
 * An interface that transforms an input string in a certain way.
 */
@FunctionalInterface
public interface ProtocolTransformer {
    /**
     * Transform a provided string following a certain format.
     * @param inputString An input string.
     * @return An optional transformed string.
     */
    Optional<String> transform(String inputString);
}

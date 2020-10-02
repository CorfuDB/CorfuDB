package org.corfudb.common.metrics.micrometer.protocoltransformer.influx;

import lombok.AllArgsConstructor;
import org.corfudb.common.metrics.micrometer.protocoltransformer.MatcherTransformer;

import java.util.Optional;

/**
 * Matcher transformer that transforms the line into the InfluxDb line protocol.
 */
public interface InfluxLineProtocolTransformer extends MatcherTransformer {
    /**
     * Measurement, tagSet and fieldSet.
     */
    int NUM_GROUPS = 3;

    /**
     * A definition of the InfluxDb data point.
     */
    @AllArgsConstructor
    class InfluxLineProtocolPoint {
        private final String measurement;
        private final String tagSet;
        private final String fieldSet;
        private final long timeStamp;

        @Override
        public String toString() {
            return String.format("%s,%s %s %s", measurement, tagSet, fieldSet, timeStamp);
        }
    }

    default Optional<String> transform(String inputString) {
        return getMatched().flatMap(matched -> {
            if (matched.groupCount() != NUM_GROUPS) {
                return Optional.empty();
            }
            String measurement = matched.group(1);
            String tagSet = matched.group(2);
            String fieldSet = matched.group(3);
            return Optional.of(new InfluxLineProtocolPoint(measurement, tagSet, fieldSet, System.nanoTime())
                    .toString());
        });
    }
}

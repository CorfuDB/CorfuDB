package org.corfudb.benchmarks.runtime.collections.helper;

import org.corfudb.benchmarks.util.DataGenerator;

public interface ValueGenerator {
    String value();

    /**
     * Holds a generated value and provides the same value always
     */
    class StaticValueGenerator implements ValueGenerator {

        private final int valueSize;
        private final String value;

        public StaticValueGenerator(int valueSize) {
            this.valueSize = valueSize;
            this.value = DataGenerator.generateDataString(valueSize);
        }

        @Override
        public String value() {
            return value;
        }
    }

    /**
     * Generates pseudo random string value every time.
     */
    class DynamicValueGenerator implements ValueGenerator {

        private final int valueSize;

        public DynamicValueGenerator(int valueSize) {
            this.valueSize = valueSize;
        }

        @Override
        public String value() {
            return DataGenerator.generateDataString(valueSize);
        }
    }
}

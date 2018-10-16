package org.corfudb.universe.scenario.spec;

import lombok.Data;

/**
 * See: <a href="http://xunitpatterns.com/Goals%20of%20Test%20Automation.html#Tests%20as%20Specification">
 * Test specification
 * </a>
 *
 * @param <T> input data
 * @param <R> action result
 */
@FunctionalInterface
public interface Spec<T, R> {
    void check(T input, R result);

    @Data
    abstract class AbstractSpec<T, R> implements Spec<T, R> {
        protected String description;

        public AbstractSpec(String description) {
            this.description = description;
        }
    }
}

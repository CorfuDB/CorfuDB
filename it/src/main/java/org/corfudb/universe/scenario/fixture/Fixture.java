package org.corfudb.universe.scenario.fixture;

/**
 * Fixture - shared test context, provides input data for tests.
 * Input data (initial parameters) can be reused in different tests and cases.
 *
 * <a href="http://xunitpatterns.com/Fixture%20Setup%20Patterns.html">xunit fixture</a>
 *
 * @param <T> data provided by the fixture
 */
@FunctionalInterface
public interface Fixture<T> {
    T data();
}

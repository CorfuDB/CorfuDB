package org.corfudb.universe.scenario;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.scenario.action.Action;
import org.corfudb.universe.scenario.action.Action.AbstractAction;
import org.corfudb.universe.scenario.fixture.Fixture;
import org.corfudb.universe.scenario.spec.Spec;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Scenario is testing Api. It uses {@link Fixture}-s and provides {@link ScenarioTestCase}-s to
 * run unit tests.
 * A scenario provides a factory method which creates TestCase.
 * The TestCase represents a group of tests, each test contains one action and one spec.
 * Scenario executes all test cases and all tests included in test cases.
 *
 * @param <D> data provided by a fixture
 * @param <T> fixture type
 */
public class Scenario<D, T extends Fixture<D>> {
    private final T fixture;

    private Scenario(T fixture) {
        this.fixture = fixture;
    }

    /**
     * Create a new scenario base on a fixture
     *
     * @param fixture a scenario fixture
     * @param <D>     data provided by the fixture
     * @param <T>     fixture type
     * @return new scenario with particular fixture
     */
    public static <D, T extends Fixture<D>> Scenario<D, T> with(T fixture) {
        return new Scenario<>(fixture);
    }

    /**
     * Create a new {@link ScenarioTestCase}
     *
     * @param testCaseFunction provides testCase api for underlying tests
     * @return scenario object
     */
    public Scenario<D, T> describe(BiConsumer<T, ScenarioTestCase<D>> testCaseFunction) {
        ScenarioTestCase<D> testCase = new ScenarioTestCase<>(fixture.data());
        testCaseFunction.accept(fixture, testCase);
        return this;
    }

    /**
     * ScenarioTestCase is a ScenarioTest-s factory, represents a group of tests. It provides api for creating new tests
     *
     * @param <T> input data
     */
    @Slf4j
    public static class ScenarioTestCase<T> {
        private final T data;

        public ScenarioTestCase(T data) {
            this.data = data;
        }

        /**
         * Factory method for creating scenario tests
         *
         * @param description  test description
         * @param actionResult describes the action result type, prevents type erasure for action result <R>
         * @param test         provides test api for underlying code
         * @param <R>          action result type
         * @return new test
         */
        public <R> ScenarioTest<T, R> it(String description, Class<R> actionResult, Consumer<ScenarioTest<T, R>> test) {
            log.info("Test: {}, action result: {}", description, actionResult.getSimpleName());
            ScenarioTest<T, R> scenarioTest = new ScenarioTest<>(data, null, description);
            test.accept(scenarioTest);
            return scenarioTest;
        }

        public ScenarioTestCase<T> it(String description, Consumer<T> specification) {
            log.info("Execute test: {}", description);
            specification.accept(data);
            return this;
        }
    }

    /**
     * Scenario test execute an action and then check the result by a spec.
     *
     * @param <T> fixture data
     * @param <R> action result
     */
    public static class ScenarioTest<T, R> {
        private final String description;
        private final T data;
        private final Action<R> action;

        public ScenarioTest(T data, Action<R> action, String description) {
            this.data = data;
            this.action = action;
            this.description = description;
        }

        public ScenarioTest<T, R> action(Action<R> action) {
            return new ScenarioTest<>(data, action, description);
        }

        public ScenarioTest<T, R> action(AbstractAction<R> action) {
            return new ScenarioTest<>(data, action, description);
        }

        /**
         * Check action result
         *
         * @param spec specification, checks action result
         */
        public ScenarioTest<T, R> check(Spec<T, R> spec) {
            spec.check(data, action.execute());
            return this;
        }
    }
}

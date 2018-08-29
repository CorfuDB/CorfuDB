package org.corfudb.universe.scenario;

import lombok.extern.slf4j.Slf4j;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.corfudb.universe.scenario.Scenario.Fixture;

public class Scenario<Data, T extends Fixture<Data>> {
    private final T fixture;

    private Scenario(T fixture) {
        this.fixture = fixture;
    }

    public static <Data, T extends Fixture<Data>> Scenario<Data, T> with(T fixture) {
        return new Scenario<>(fixture);
    }

    public Scenario<Data, T> describe(BiConsumer<T, ScenarioTestCase<Data>> test) {
        ScenarioTestCase<Data> testCase = new ScenarioTestCase<>(fixture.data());
        test.accept(fixture, testCase);
        return this;
    }

    @FunctionalInterface
    public interface Fixture<T> {
        T data();
    }

    /**
     * Provides an interface for executable actions used in scenarios. Actions can be either Tasks or Faults.
     * <p>
     * Task: a command that changes the state of test framework elements
     * Fault: a command that is state change representing a faulty state of test framework elements
     */
    @FunctionalInterface
    public interface Action<T, R> {
        R execute(T data);
    }

    @FunctionalInterface
    public interface Spec<T, R> {
        void check(T input, R result);
    }

    @Slf4j
    public static class ScenarioTestCase<T> {
        private final T data;

        public ScenarioTestCase(T data) {
            this.data = data;
        }

        public <R> ScenarioTest<T, R> it(String description, Class<R> actionResult, Consumer<ScenarioTest<T, R>> test) {
            log.info("Test: {}, action result: {}", description, actionResult.getSimpleName());
            ScenarioTest<T, R> scenarioTest = new ScenarioTest<>(data, null);
            test.accept(scenarioTest);
            return scenarioTest;
        }
    }

    public static class ScenarioTest<T, R> {
        private final T data;
        private final Action<T, R> action;

        public ScenarioTest(T data, Action<T, R> action) {
            this.data = data;
            this.action = action;
        }

        public ScenarioTest<T, R> action(Action<T, R> action) {
            return new ScenarioTest<>(data, action);
        }

        public void check(Spec<T, R> spec) {
            spec.check(data, action.execute(data));
        }
    }
}

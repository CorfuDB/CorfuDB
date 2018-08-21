package org.corfudb.universe.action;

/**
 * Provides an interface for executable actions used in scenarios. Actions can be either Tasks or Faults.
 *
 * Task: a command that changes the state of test framework elements
 * Fault: a command that is state change representing a faulty state of test framework elements
 */
public interface Action {
    void execute();

    interface Task extends Action {

    }

    interface Fault extends Action {

    }
}

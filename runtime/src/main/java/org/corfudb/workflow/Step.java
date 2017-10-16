package org.corfudb.workflow;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import lombok.Getter;

/**
 * Independent process to be executed with a set of parameters.
 * A series of sequential steps form a workflow.
 *
 * <p>Created by zlokhandwala on 10/16/17.
 */
public class Step<T, R> {

    /**
     * Unique step Id.
     */
    public final UUID stepId = UUID.randomUUID();

    /**
     * Name of the step. Used to recreate the step on a different node.
     */
    public final String name;

    /**
     * Process to be executed.
     */
    private final Function<T, R> process;

    /**
     * Timeout in milliseconds
     */
    @Getter
    private final long timeout;

    private T parameters;

    /**
     * Sets the parameters to be passed on to the step.
     *
     * @param parameters Parameters of type T
     * @return Step instance.
     */
    public Step<T, R> setParameters(T parameters) {
        this.parameters = parameters;
        return this;
    }

    final CompletableFuture<R> future = new CompletableFuture<>();

    public Step(String name, Function<T, R> process) {
        this(name, process, Long.MAX_VALUE);
    }

    public Step(String name, Function<T, R> process, long timeout) {
        this.name = name;
        this.process = process;
        this.timeout = timeout;
    }

    /**
     * Executes the process with set parameters.
     * In future: The result of this process can be linked to the next step.
     *
     * @return Completable future which completes on successful execution of the process.
     */
    public CompletableFuture execute() {
        try {
            R result = process.apply(parameters);
            future.complete(result);
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }
}

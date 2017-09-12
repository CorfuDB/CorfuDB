package org.corfudb.workflow;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import lombok.Getter;

public class Step<T, R> {

    public final UUID stepId = UUID.randomUUID();
    public final String name;


    private final Function<T, R> process;

    // Timeout in milliseconds
    @Getter
    private final long timeout;

    private T parameters;

    public Step<T, R> setParameters(T parameters) {
        this.parameters = parameters;
        return this;
    }

    // Status of the Step
    public final CompletableFuture<R> future = new CompletableFuture<>();

    public Step(String name, Function<T, R> process) {
        this(name, process, Long.MAX_VALUE);
    }

    public Step(String name, Function<T, R> process, long timeout) {
        this.name = name;
        this.process = process;
        this.timeout = timeout;
    }

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

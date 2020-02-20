package org.corfudb.runtime.utils;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.util.CFUtils;

import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExceptionContextTest extends AbstractViewTest {

    private static class DummyCausingException extends RuntimeException {
        DummyCausingException(String message) {
            super(message);
        }
    }

    @Test
    public void validateCallerStackTracePresent() throws InterruptedException {
        final CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
            throw new DummyCausingException("Dummy causing exception");
        });

        ExecutionException originalExecutionException = null;
        try {
            future.get();
        } catch (ExecutionException e) {
            originalExecutionException = e;
        }
        final StackTraceElement[] causeOriginalStackTrace = originalExecutionException
                                                                .getCause().getStackTrace();

        RuntimeException causeFromGetUninterruptibly = null;
        try {
            CFUtils.getUninterruptibly(future, RuntimeException.class);
        } catch (DummyCausingException e) {
            causeFromGetUninterruptibly = e;
        }
        final List<StackTraceElement> causeFromGetUninterruptiblyStackTrace = Arrays.asList(
                causeFromGetUninterruptibly.getStackTrace()
        );

        final int callerContextEndIndex = causeFromGetUninterruptiblyStackTrace.indexOf(
                new StackTraceElement("Dummy stack frame", "--- End caller context ---", null, -1)
        );
        assertNotEquals(callerContextEndIndex, -1);

        final List<String> expectedStackTraceContents = Arrays.asList(
                "CompletableFuture.reportGet",
                "CompletableFuture.get",
                "CFUtils.getUninterruptibly",
                "ExceptionContextTest.validateCallerStackTracePresent"
        );

        expectedStackTraceContents.forEach(contents -> {
            final String className = contents.split("\\.")[0];
            final String methodName = contents.split("\\.")[1];
            final boolean present = causeFromGetUninterruptiblyStackTrace
                                    .subList(0, callerContextEndIndex)
                                    .stream()
                                    .anyMatch(stackTraceElement -> {
                                        return stackTraceElement.getClassName().endsWith(className)
                                            && stackTraceElement.getMethodName().equals(methodName);
                                    });
            assertTrue(
                    present,
                    String.format("Could not find expected stack trace element for caller: %s",
                                  contents)
            );
        });

        for (int i = 0; i < causeOriginalStackTrace.length; ++i) {
            assertEquals(
                    causeOriginalStackTrace[i],
                    causeFromGetUninterruptiblyStackTrace.get(callerContextEndIndex + i + 1)
            );
        }
    }
}

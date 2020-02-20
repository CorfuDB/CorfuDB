package org.corfudb.runtime.utils;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Utils;

import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExceptionContextTest extends AbstractViewTest {

    private static class DummyCausingException extends RuntimeException {
        DummyCausingException(String message) {
            super(message);
        }
    }

    // make a static in Utils.java?
    private static final StackTraceElement dummyStackFrameEntry = new StackTraceElement(
            "Dummy stack frame", "--- End caller context ---", null, -1);

    /**
     * Verifies a causing exception from another thread thrown from CFUtils.getUninterruptibly has
     * caller's stacktrace prepended
     */
    @Test
    public void validateCallerStackTracePresentInGetUninterruptibly() throws InterruptedException {
        final CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
            throw new DummyCausingException("Dummy causing exception");
        });

        final ExecutionException originalExecutionException = assertThrows(
                ExecutionException.class,
                () -> {
                    future.get();
                }
        );
        final StackTraceElement[] causeOriginalStackTrace = originalExecutionException
                                                                .getCause()
                                                                .getStackTrace();

        final DummyCausingException causeFromGetUninterruptibly = assertThrows(
                DummyCausingException.class,
                () -> {
                    CFUtils.getUninterruptibly(future, RuntimeException.class);
                }
        );
        final List<StackTraceElement> causeFromGetUninterruptiblyStackTrace = Arrays.asList(
                causeFromGetUninterruptibly.getStackTrace()
        );

        // check for dummy stack separator entry
        final int callerContextEndIndex = causeFromGetUninterruptiblyStackTrace.indexOf(
                dummyStackFrameEntry
        );
        assertNotEquals(callerContextEndIndex, -1);

        // check for caller's stack trace entries
        final List<String> expectedStackTraceContents = Arrays.asList(
                "CompletableFuture.reportGet",
                "CompletableFuture.get",
                "CFUtils.getUninterruptibly",
                "ExceptionContextTest.validateCallerStackTracePresentInGetUninterruptibly"
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

        // check for causing exception's original stack trace
        for (int i = 0; i < causeOriginalStackTrace.length; ++i) {
            assertEquals(
                    causeOriginalStackTrace[i],
                    causeFromGetUninterruptiblyStackTrace.get(callerContextEndIndex + i + 1)
            );
        }
    }

    /**
     * Verifies a causing exception from another thread extracted using
     * Utils.extractCauseWithCompleteStacktrace has outer exception's stacktrace prepended
     */
    @Test
    public void validateCallerStackTracePresentInExtractCause() throws InterruptedException {
        final CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
            throw new DummyCausingException("Dummy causing exception");
        });

        final ExecutionException originalExecutionException = assertThrows(
                ExecutionException.class,
                () -> {
                    future.get();
                }
        );
        final StackTraceElement[] originalExecutionExceptionStackTrace = originalExecutionException
                                                                            .getStackTrace();
        final StackTraceElement[] causeOriginalStackTrace = originalExecutionException
                                                                .getCause()
                                                                .getStackTrace();

        final Throwable causeFromExtractCause = Utils.extractCauseWithCompleteStacktrace(
                originalExecutionException
        );
        assertTrue(causeFromExtractCause instanceof DummyCausingException);
        final StackTraceElement[] causeFromExtractCauseStackTrace = causeFromExtractCause
                                                                        .getStackTrace();

        // check caller's track trace present
        for (int i = 0; i < originalExecutionExceptionStackTrace.length; ++i) {
            assertEquals(
                    originalExecutionExceptionStackTrace[i],
                    causeFromExtractCauseStackTrace[i]
            );
        }

        // check for dummy stack separator entry
        assertEquals(
                causeFromExtractCauseStackTrace[originalExecutionExceptionStackTrace.length],
                dummyStackFrameEntry
        );

        // check for causing exception's original stack trace
        for (int i = 0; i < causeOriginalStackTrace.length; ++i) {
            assertEquals(
                    causeOriginalStackTrace[i],
                    causeFromExtractCauseStackTrace[originalExecutionExceptionStackTrace.length + 1 + i]
            );
        }
    }

    /**
     * Call Utils.extractCauseWithCompleteStacktrace on a throwable with no cause
     */
    @Test
    public void extractNullCause() {
        assertEquals(null,
                    Utils.extractCauseWithCompleteStacktrace(
                        new DummyCausingException("Dummy causing exception")
                    )
        );
    }
}

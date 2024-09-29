package org.corfudb.util;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * Created by mwei on 3/24/16.
 */
@Slf4j
public class LambdaUtils {

    private LambdaUtils() {
        // prevent instantiation of this class
    }

    /**
     * Suppresses all exceptions. This is used for scheduling tasks to ScheduledExecutorService.
     * ScheduledExecutorService crashes in case a scheduled thread throws an Exception.
     *
     * @param runnable Task whose exceptions are to be suppressed.
     */
    public static void runSansThrow(Runnable runnable) {
        try {
            runnable.run();
        } catch (Throwable e) {
            log.error("runSansThrow: Suppressing exception while executing runnable: ", e);
        }
    }

    @Builder
    public static class BiOptional<F, S> {
        @NonNull
        private final Optional<F> first;
        @NonNull
        private final Optional<S> second;

        private BiOptional(@NonNull Optional<F> first, @NonNull Optional<S> second) {
            this.first = first;
            this.second = second;
        }

        public static <F, S> BiOptional<F, S> of(@NonNull Optional<F> first, @NonNull Optional<S> second) {
            return new BiOptional<>(first, second);
        }

        public void ifPresent(BiConsumer<? super F, ? super S> action) {
            first.ifPresent(firstVal -> {
                second.ifPresent(secondVal -> {
                    action.accept(firstVal, secondVal);
                });
            });
        }

        public boolean isEmpty() {
            return !first.isPresent() || !second.isPresent();
        }
    }
}

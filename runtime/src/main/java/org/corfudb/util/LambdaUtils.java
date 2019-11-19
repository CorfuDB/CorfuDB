package org.corfudb.util;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleConsumer;
import java.util.function.DoubleFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleSupplier;
import java.util.function.DoubleToIntFunction;
import java.util.function.DoubleToLongFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.IntBinaryOperator;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.IntSupplier;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;
import java.util.function.IntUnaryOperator;
import java.util.function.LongBinaryOperator;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;
import java.util.function.LongSupplier;
import java.util.function.LongToDoubleFunction;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.ObjDoubleConsumer;
import java.util.function.ObjIntConsumer;
import java.util.function.ObjLongConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleBiFunction;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntBiFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongBiFunction;
import java.util.function.ToLongFunction;
import java.util.function.UnaryOperator;

/**
 * Created by mwei on 3/24/16.
 */
@Slf4j
public class LambdaUtils {

    private LambdaUtils() {
        // prevent instantiation of this class
    }

    private static final Map<Class<Object>,
            LambdaResolver<Object>> dispatchMap = generateDispatchMap();

    @SuppressWarnings("unchecked")
    private static Map<Class<Object>, LambdaResolver<Object>> generateDispatchMap() {
        ImmutableMap.Builder<Class<Object>, LambdaResolver<Object>> builder =
                ImmutableMap.<Class<Object>, LambdaResolver<Object>>builder();

        typedPut(builder, BiConsumer.class, (f, a) -> {
            f.accept(a[0], a[1]);
            return null;
        });
        typedPut(builder, BiFunction.class, (f, a) -> f.apply(a[0], a[1]));
        typedPut(builder, BinaryOperator.class, (f, a) -> f.apply(a[0], a[1]));
        typedPut(builder, BiPredicate.class, (f, a) -> f.test(a[0], a[1]));
        typedPut(builder, BooleanSupplier.class, (f, a) -> f.getAsBoolean());
        typedPut(builder, Consumer.class, (f, a) -> {
            f.accept(a[0]);
            return null;
        });
        typedPut(builder, DoubleBinaryOperator.class, (f, a) -> f.applyAsDouble((double) a[0],
                (double) a[1]));
        typedPut(builder, DoubleConsumer.class, (f, a) -> {
            f.accept((double) a[0]);
            return null;
        });
        typedPut(builder, DoubleFunction.class, (f, a) -> f.apply((double) a[0]));
        typedPut(builder, DoublePredicate.class, (f, a) -> f.test((double) a[0]));
        typedPut(builder, DoubleSupplier.class, (f, a) -> f.getAsDouble());
        typedPut(builder, DoubleToIntFunction.class, (f, a) -> f.applyAsInt((double) a[0]));
        typedPut(builder, DoubleToLongFunction.class, (f, a) -> f.applyAsLong((double) a[0]));
        typedPut(builder, DoubleUnaryOperator.class, (f, a) -> f.applyAsDouble((double) a[0]));
        typedPut(builder, Function.class, (f, a) -> f.apply(a[0]));
        typedPut(builder, IntBinaryOperator.class, (f, a) -> f.applyAsInt((int) a[0], (int) a[1]));
        typedPut(builder, IntConsumer.class, (f, a) -> {
            f.accept((int) a[0]);
            return null;
        });
        typedPut(builder, IntFunction.class, (f, a) -> f.apply((int) a[0]));
        typedPut(builder, IntPredicate.class, (f, a) -> f.test((int) a[0]));
        typedPut(builder, IntSupplier.class, (f, a) -> f.getAsInt());
        typedPut(builder, IntToDoubleFunction.class, (f, a) -> f.applyAsDouble((int) a[0]));
        typedPut(builder, IntToLongFunction.class, (f, a) -> f.applyAsLong((int) a[0]));
        typedPut(builder, IntUnaryOperator.class, (f, a) -> f.applyAsInt((int) a[0]));
        typedPut(builder, LongBinaryOperator.class, (f, a) -> f.applyAsLong((long) a[0],
                (long) a[1]));
        typedPut(builder, LongConsumer.class, (f, a) -> {
            f.accept((long) a[0]);
            return null;
        });
        typedPut(builder, LongFunction.class, (f, a) -> f.apply((long) a[0]));
        typedPut(builder, LongPredicate.class, (f, a) -> f.test((long) a[0]));
        typedPut(builder, LongSupplier.class, (f, a) -> f.getAsLong());
        typedPut(builder, LongToDoubleFunction.class, (f, a) -> f.applyAsDouble((long) a[0]));
        typedPut(builder, LongToIntFunction.class, (f, a) -> f.applyAsInt((long) a[0]));
        typedPut(builder, LongUnaryOperator.class, (f, a) -> f.applyAsLong((long) a[0]));
        typedPut(builder, ObjDoubleConsumer.class, (f, a) -> {
            f.accept(a[0], (double) a[1]);
            return null;
        });
        typedPut(builder, ObjIntConsumer.class, (f, a) -> {
            f.accept(a[0], (int) a[1]);
            return null;
        });
        typedPut(builder, ObjLongConsumer.class, (f, a) -> {
            f.accept(a[0], (long) a[1]);
            return null;
        });
        typedPut(builder, Predicate.class, (f, a) -> f.test(a[0]));
        typedPut(builder, Supplier.class, (f, a) -> f.get());
        typedPut(builder, ToDoubleBiFunction.class, (f, a) -> f.applyAsDouble(a[0], a[1]));
        typedPut(builder, ToDoubleFunction.class, (f, a) -> f.applyAsDouble(a[0]));
        typedPut(builder, ToIntBiFunction.class, (f, a) -> f.applyAsInt(a[0], a[1]));
        typedPut(builder, ToIntFunction.class, (f, a) -> f.applyAsInt(a[0]));
        typedPut(builder, ToLongBiFunction.class, (f, a) -> f.applyAsLong(a[0], a[1]));
        typedPut(builder, ToLongFunction.class, (f, a) -> f.applyAsLong(a[0]));
        typedPut(builder, UnaryOperator.class, (f, a) -> f.apply(a[0]));

        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private static <T, U> ImmutableMap.Builder<Class<T>, LambdaResolver<T>> typedPut(
            ImmutableMap.Builder<Class<T>, LambdaResolver<T>> builder,
            Class<U> cls,
            LambdaResolver<U> fn) {
        return builder.put((Class<T>) cls, (LambdaResolver<T>) fn);
    }

    /**
     * This function executes an unknown lambda from Java's broken (IMO) functional interface.
     */
    public static Object executeUnknownLambda(Object unknownLambda, Object... arguments) {
        return dispatchMap.entrySet().stream()
                .filter(e -> e.getKey().isInstance(unknownLambda))
                .findFirst().get().getValue().resolve(unknownLambda, arguments);
    }

    @FunctionalInterface
    interface LambdaResolver<T> {
        Object resolve(T resolver, Object[] args);
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
        } catch (Throwable t) {
            log.error("runSansThrow: Suppressing exception while executing runnable: ", t);
        }
    }
}

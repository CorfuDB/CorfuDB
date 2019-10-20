package org.corfudb.common.streamutils;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;

import java.util.Iterator;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * A utility class to perform operation on a stream of elements.
 */
public class StreamUtils {

    @Getter
    public static class StreamHeadAndTail<T> {
        private final Optional<T> head;
        private final Stream<Optional<T>> tail;

        public StreamHeadAndTail(Optional<T> head, Stream<Optional<T>> tail) {
            this.head = head;
            this.tail = tail;
        }

        public StreamHeadAndTail() {
            this.head = Optional.empty();
            this.tail = Stream.of();
        }
    }

    private static StreamUtils ourInstance = new StreamUtils();

    public static StreamUtils getInstance() {
        return ourInstance;
    }

    private StreamUtils() {
    }

    /**
     * Splits the stream into the head and the tail.
     *
     * @param stream An instance of a stream.
     * @param <T>    A generic parameter.
     * @return An instance of a split stream.
     */
    public static <T> StreamHeadAndTail<T> splitStream(final Stream<T> stream) {
        Stream<Optional<T>> tail = stream.map(Optional::ofNullable);
        return splitTail(tail);

    }

    /**
     * Splits the tail of a stream into the head and a tail.
     * @param tail An instance of a stream.
     * @param <T> A generic parameter.
     * @return An instance of a split stream.
     */
    public static <T> StreamHeadAndTail<T> splitTail(final Stream<Optional<T>> tail) {
        Iterator<Optional<T>> iterator = tail.iterator();

        Supplier<Optional<T>> defaultTailGenerator = () -> {
            if (iterator.hasNext()) {
                return iterator.next();
            } else {
                return Optional.empty();
            }
        };
        if (iterator.hasNext()) {
            return new StreamHeadAndTail<>(iterator.next(), Stream.generate(defaultTailGenerator));
        } else {
            return new StreamHeadAndTail<>(Optional.empty(), Stream.generate(defaultTailGenerator));
        }
    }
}

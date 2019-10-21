package org.corfudb.common.streamutils;

import lombok.Getter;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * A utility class to perform operations on a stream of elements.
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
     * @param bound  The expected size of the entire stream.
     * @param <T>    A generic parameter.
     * @return An instance of a split stream.
     */
    public static <T> StreamHeadAndTail<T> splitStream(final Stream<T> stream, long bound) {
        Stream<Optional<T>> tail = stream.map(Optional::ofNullable);
        return splitTail(tail, bound);

    }

    /**
     * Splits the tail of a stream into the head and a tail.
     *
     * @param tail  An instance of a stream.
     * @param bound A size of this stream.
     * @param <T>   A generic parameter.
     * @return An instance of a split stream.
     */
    public static <T> StreamHeadAndTail<T> splitTail(
            final Stream<Optional<T>> tail, long bound) {
        Iterator<Optional<T>> iterator = tail.iterator();

        Supplier<Optional<T>> defaultTailGenerator = () -> {
            if (iterator.hasNext()) {
                return iterator.next();
            } else {
                return Optional.empty();
            }
        };
        if (iterator.hasNext()) {
            return new StreamHeadAndTail<>(iterator.next(),
                    Stream.generate(defaultTailGenerator)
                            .filter(Optional::isPresent)
                            .limit(bound - 1));
        } else {
            return new StreamHeadAndTail<>(Optional.empty(), Stream.empty());
        }


    }
}

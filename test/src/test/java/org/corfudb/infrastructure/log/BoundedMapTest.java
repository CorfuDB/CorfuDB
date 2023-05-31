package org.corfudb.infrastructure.log;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class BoundedMapTest {

    @Test
    public void testIndexSet() {
        BoundedMap index = new BoundedMap(0, 10_000);
        assertThat(index.contains(10L)).isFalse();
        assertThat(index.set(10, 1000L)).isTrue();
        assertThat(index.set(10, 1000L)).isFalse();
        assertThat(index.get(10)).isEqualTo(1000L);
        assertThat(index.capacity()).isEqualTo(10_000);
    }

    @Test
    public void testIndexOutOfRange() {
        BoundedMap index = new BoundedMap(0, 10);
        String errorMsg = "10 not in [0, 10)";
        assertThatThrownBy(() -> index.contains(10L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(errorMsg);

        assertThatThrownBy(() -> index.set(10, 1000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(errorMsg);

        assertThatThrownBy(() -> index.get(10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(errorMsg);

        assertThatThrownBy(() -> index.set(1, Long.MIN_VALUE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid %s value", Long.MIN_VALUE);

        assertThatThrownBy(() -> index.set(Long.MIN_VALUE, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testInvalidSet() {
        BoundedMap index = new BoundedMap(0, 10);
        assertThatThrownBy(() -> index.set(0, BoundedMap.NOT_SET))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testConstructorConditions() {
        assertThatThrownBy(() -> new BoundedMap(-1, 0))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new BoundedMap(0, 0))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new BoundedMap(Long.MAX_VALUE, 1))
                .isInstanceOf(ArithmeticException.class);
    }

    @Test
    public void testEmptyIterator() {
        BoundedMap index = new BoundedMap(0, 10);

        assertThat(index.iterator().hasNext())
                .isFalse();

        assertThat(index.set(0L, 1L)).isTrue();

        Iterator<Long> iter = index.iterator();

        assertThat(iter.hasNext())
                .isTrue();
        assertThat(iter.next()).isEqualTo(0L);
        assertThat(iter.hasNext()).isFalse();
    }

    @Test
    public void testSparseIndex() {
        BoundedMap index = new BoundedMap(0, 10);

        assertThat(index.iterator().hasNext())
                .isFalse();

        assertThat(index.set(5L, 5L)).isTrue();
        assertThat(index.set(9L, 9L)).isTrue();

        Iterator<Long> iter = index.iterator();

        assertThat(iter.hasNext())
                .isTrue();
        assertThat(iter.next()).isEqualTo(5L);
        assertThat(iter.next()).isEqualTo(9L);
        assertThat(iter.hasNext()).isFalse();
    }

    @Test
    public void testIterable() {
        BoundedMap index = new BoundedMap(0, 10);

        for (long i : index.iterable()) {
            assertThat(i).isNotEqualTo(i);
        }

        assertThat(index.set(1L, 1L)).isTrue();
        assertThat(index.set(2L, 2L)).isTrue();

        List<Long> contents = new ArrayList<>();

        for (long l : index.iterable()) {
            contents.add(l);
        }

        assertThat(contents).containsExactly(1L, 2L);
    }
}

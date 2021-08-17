package org.corfudb.generator.distributions;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.generator.distributions.Streams.StreamId;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * This class implements the distribution of keys that can be inserted
 * into a map.
 * <p>
 * Created by maithem on 7/14/17.
 */
public class Keys implements DataSet<Keys.KeyId> {
    private final Set<KeyId> mapKeys;
    private final int numKeys;

    public Keys(int num) {
        mapKeys = new HashSet<>();
        numKeys = num;
    }

    @Override
    public void populate() {
        for (int key = 0; key < numKeys; key++) {
            mapKeys.add(new KeyId(key));
        }
    }

    @Override
    public List<KeyId> getDataSet() {
        if (mapKeys.isEmpty()) {
            throw new IllegalStateException("Keys were not populated");
        }

        return new ArrayList<>(mapKeys);
    }

    @EqualsAndHashCode
    @AllArgsConstructor
    public static class KeyId {
        private final int key;

        public String getKey() {
            return String.valueOf(key);
        }

        @Override
        public String toString() {
            return getKey();
        }
    }

    @Builder
    @EqualsAndHashCode
    @AllArgsConstructor
    @ToString
    @Getter
    public static class FullyQualifiedKey {
        private final KeyId keyId;
        private final StreamId tableId;
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @EqualsAndHashCode
    public static class Version implements Comparable<Version> {
        private static final ConcurrentMap<Long, Version> REGISTRY = new ConcurrentHashMap<>();
        private static final Function<Long, Version> FACTORY = Version::new;

        @Getter
        private final long ver;

        public static Version build(long version){
            return REGISTRY.computeIfAbsent(version, FACTORY);
        }

        public static Version noVersion() {
            return build(-1);
        }

        @Override
        public int compareTo(Version other) {
            return Long.compare(ver, other.ver);
        }

        @Override
        public String toString() {
            return String.valueOf(ver);
        }
    }
}

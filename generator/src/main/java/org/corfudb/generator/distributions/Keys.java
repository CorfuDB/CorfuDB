package org.corfudb.generator.distributions;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.corfudb.generator.distributions.Streams.StreamId;
import org.corfudb.generator.state.KeysState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class implements the distribution of keys that can be inserted
 * into a map.
 * <p>
 * Created by maithem on 7/14/17.
 */
public class Keys implements DataSet<Keys.KeyId> {
    private final Set<KeyId> mapKeys;
    private final int numKeys;
    private final Map<KeyId, KeysState> keysState;

    public Keys(int num) {
        mapKeys = new HashSet<>();
        numKeys = num;
        keysState = new HashMap<>();
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
    }

    @AllArgsConstructor
    public static class FullyQualifiedKey {
        private final KeyId keyId;
        private final StreamId tableId;
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    public static class Version implements Comparable<Version> {
        private final int version;

        @Override
        public int compareTo(Version other) {
            return Integer.compare(version, other.version);
        }
    }
}

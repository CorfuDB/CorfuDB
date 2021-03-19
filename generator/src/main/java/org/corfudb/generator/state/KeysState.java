package org.corfudb.generator.state;

import lombok.AllArgsConstructor;

import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.corfudb.generator.distributions.Keys.*;

public class KeysState {

    private final Map<FullyQualifiedKey, VersionedKey> state;

    public static class VersionedKey {
        //private final FullyQualifiedKey key;
        private final SortedMap<Version, KeyEntry> history = new TreeMap<>();

        public void put(KeyEntry keyEntry) {
            history.put(keyEntry.version, keyEntry);
        }
    }

    @AllArgsConstructor
    public static class KeyEntry {
        private final Version version;
        private final String value;

        private final String threadId;
        private final String clientId;

        private final Optional<TxMetaInfo> txInfo;
    }

    @AllArgsConstructor
    public static class TxMetaInfo {
        private final int id;
        private final int start;
        private final int end;
    }
}

package org.corfudb.generator.state;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.corfudb.generator.distributions.Keys;

import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static org.corfudb.generator.distributions.Keys.FullyQualifiedKey;
import static org.corfudb.generator.distributions.Keys.Version;

public class KeysState {

    private final ConcurrentMap<FullyQualifiedKey, VersionedKey> keys = new ConcurrentHashMap<>();
    private final ConcurrentMap<ThreadName, Version> versionsByThread = new ConcurrentHashMap<>();

    public VersionedKey get(FullyQualifiedKey key) {
        return keys.get(key);
    }

    public void updateThreadLatestVersion(ThreadName thread, Version version) {
        versionsByThread.put(thread, version);
    }

    public Version getThreadLatestVersion(ThreadName thread) {
        return versionsByThread.get(thread);
    }

    public static class VersionedKey {
        //private final FullyQualifiedKey key;
        private final SortedMap<Keys.Version, KeyEntry> history = new TreeMap<>();

        public KeyEntry get(Keys.Version version){
            return history.get(version);
        }

        public void put(KeyEntry keyEntry) {
            history.put(keyEntry.version, keyEntry);
        }
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    public static class KeyEntry {
        private final Version version;
        @Getter
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

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @EqualsAndHashCode
    public static class ThreadName {
        private static final ConcurrentMap<String, ThreadName> REGISTRY = new ConcurrentHashMap<>();
        private static final Function<String, ThreadName> FACTORY = ThreadName::new;

        private final String name;

        public static ThreadName buildFromCurrentThread() {
            return REGISTRY.computeIfAbsent(Thread.currentThread().getName(), FACTORY);
        }
    }
}

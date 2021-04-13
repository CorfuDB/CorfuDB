package org.corfudb.generator.state;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
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

    public void put(FullyQualifiedKey key, KeyEntry entry){
        keys.get(key).put(entry);
    }

    public void updateThreadLatestVersion(ThreadName thread, Version version) {
        versionsByThread.put(thread, version);
    }

    public Version getThreadLatestVersion(ThreadName thread) {
        return versionsByThread.get(thread);
    }

    public boolean contains(FullyQualifiedKey fqKey) {
        return keys.containsKey(fqKey);
    }

    public static class VersionedKey {
        private final SortedMap<Keys.Version, KeyEntry> history = new TreeMap<>();

        public KeyEntry get(Keys.Version version){
            return history.get(version);
        }

        public void put(KeyEntry keyEntry) {
            history.put(keyEntry.snapshotId.version, keyEntry);
        }
    }

    @Builder
    @ToString
    public static class SnapshotId {
        @NonNull
        private final ThreadName threadId;
        @NonNull
        @Setter
        @Getter
        private Version version;
        @NonNull
        private final String clientId;
    }

    @Builder
    @EqualsAndHashCode
    @ToString
    public static class KeyEntry {
        @NonNull
        private final SnapshotId snapshotId;
        @NonNull
        @Getter
        private final Optional<String> value;
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @EqualsAndHashCode
    @ToString
    public static class ThreadName {
        private static final ConcurrentMap<String, ThreadName> REGISTRY = new ConcurrentHashMap<>();
        private static final Function<String, ThreadName> FACTORY = ThreadName::new;

        private final String name;

        public static ThreadName buildFromCurrentThread() {
            return REGISTRY.computeIfAbsent(Thread.currentThread().getName(), FACTORY);
        }
    }
}

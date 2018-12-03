package org.corfudb.util;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * Created by Maithem on 1/7/18.
 */
@Slf4j
public class LRU {
    Map<Long, CacheEntry> map = new ConcurrentHashMap();

    ReadWriteLock rwLock = new ReentrantReadWriteLock();

    AtomicLong currentTimestamp = new AtomicLong(0l);

    Function<Long, ILogData> loadKey;

    Function<Iterable<Long>, Map<Long, ILogData>> loadKeys;

    int cacheSize;

    public LRU(int size, Function<Long, ILogData> loadKey, Function<Iterable<Long>, Map<Long, ILogData>> loadKeys) {
        this.cacheSize = size;
        this.loadKey = loadKey;
        this.loadKeys = loadKeys;
    }

    public void invalidateAll() {
        try {
            rwLock.writeLock().lock();
            map.clear();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Nonnull
    ILogData load(@Nonnull Long key) {
        return loadKey.apply(key);
    }

    @Nonnull
    public Map<Long, ILogData> load(@Nonnull Iterable<Long> keys) {
        return loadKeys.apply(keys);
    }

    void checkGC() {
        if (map.size() == cacheSize) {
            long window = currentTimestamp.get() - (cacheSize / 2);
            long evicted = 0;
            for (Map.Entry<Long, CacheEntry> entry : map.entrySet()) {
                if (entry.getValue().getTimestamp() < window) {
                    map.remove(entry.getKey());
                    evicted++;
                }
            }

            log.info("checkGC: evicted {} entries", evicted);
        }
    }

    void addToCache(@Nonnull Long key, @Nonnull ILogData val) {
        try {
            rwLock.writeLock().lock();
            checkGC();
            map.put(key, new CacheEntry(currentTimestamp.getAndIncrement(), val));
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    void addToCache(@Nonnull Map<Long, ILogData> vals) {
        try {
            rwLock.writeLock().lock();
            checkGC();
            // TODO(Maithem): this can potentially overflow
            for (Map.Entry<Long, ILogData> entry : vals.entrySet()) {
                map.put(entry.getKey(), new CacheEntry(currentTimestamp.getAndIncrement(),
                        entry.getValue()));
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Nonnull
    public Map<Long, ILogData> getAll(@Nonnull Iterable<? extends Long> keys) {
        Set<Long> addressesToLoad = new HashSet<>();
        Map<Long, ILogData> ret = new HashMap<>();

        for (Long key : keys) {
            CacheEntry entry = map.get(key);
            if (entry == null) {
                addressesToLoad.add(key);
            } else {
                ret.put(key,  entry.getVal());
                try {
                    rwLock.readLock().lock();
                    map.put(key, new CacheEntry(currentTimestamp.getAndIncrement(),
                             entry.getVal()));
                } finally {
                    rwLock.readLock().unlock();
                }
            }
        }

        Map<Long, ILogData> loaded = load(addressesToLoad);
        addToCache(loaded);
        ret.putAll(loaded);
        return ret;
    }

    @CheckForNull
    public ILogData get(@Nonnull Long key) {
        CacheEntry v = map.get(key);
        if (v == null) {
            ILogData val = load(key);
            addToCache(key, val);
            return val;
        } else {
            try {
                rwLock.readLock().lock();
                // Cache size doesn't change, just update the timestamp
                // TODO(Maithem) Don't generate new cache entries on every read
                map.put(key, new CacheEntry(currentTimestamp.getAndIncrement(), v.getVal()));
                return v.getVal();
            } finally {
                rwLock.readLock().unlock();
            }
        }
    }

    public void put(@Nonnull Long key, @Nonnull ILogData val) {
        addToCache(key, val);
    }

    @Data
    class CacheEntry {
        final long timestamp;
        final ILogData val;
    }
}

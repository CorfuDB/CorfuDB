package org.corfudb.runtime.object;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import lombok.extern.slf4j.Slf4j;

import java.lang.ref.SoftReference;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


/**
 * MVOCache is the centralized container that holds the reference to
 * all MVOs. Overall it provides put and get APIs and manages the
 * Cache-related properties (LRU) under the hood.
 *
 */
@Slf4j
public class MVOCache {

    // Both objectMap and objectCache hold strong references to objects
    // key is basically a pair of (objectId, version)
    // value is the actually MVO object
    private final Cache<VersionedObjectIdentifier, ICorfuSMR> objectCache;

    // This objectMap is updated at two places
    // 1) put() which puts a new version of some object to the objectMap
    // 2) as a side effect of put(), some old versions are evicted from the cache
    // TODO: access/mutation to objectMap should be synchronized
    // Q: Can we use objectCache.asMap() instead of maintaining an external view?
    // A: No. Although asMap() returns a thread-safe weakly-consistent map, it is
    //    not a tree structure so it's very inefficient to implement headMap() and
    //    floorEntry() on top of it.
    private final ConcurrentHashMap<UUID, TreeMap<Long, ICorfuSMR>> objectMap = new ConcurrentHashMap<>();

    private final long DEFAULT_MAX_CACHE_ENTRIES = 50000;
    private final long DEAFULT_CACHE_EXPIRY_TIME_IN_SECONDS = 300;

    public MVOCache() {

        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        cacheBuilder.maximumSize(DEFAULT_MAX_CACHE_ENTRIES);
        objectCache = cacheBuilder.expireAfterAccess(DEAFULT_CACHE_EXPIRY_TIME_IN_SECONDS, TimeUnit.SECONDS)
                .expireAfterWrite(DEAFULT_CACHE_EXPIRY_TIME_IN_SECONDS, TimeUnit.SECONDS)
                .removalListener(this::handleEviction)
                .recordStats()
                .build();
    }

    private void handleEviction(RemovalNotification<VersionedObjectIdentifier, Object> notification) {
        VersionedObjectIdentifier voId = notification.getKey();
        int evictCount = prefixEvict(voId);
        log.info("evicted {} versions for object {} which is older than version {}",
                evictCount, voId.getObjectId(), voId.getVersion());
    }

    /**
     * Evict all versions of the given object up to the target version
     *
     * @param voId  the object and version to perform prefixEvict
     * @return number of versions that has been evicted
     */
    private int prefixEvict(VersionedObjectIdentifier voId) {
        Map<Long, ICorfuSMR> headMap = objectMap.get(voId.getObjectId())
                .headMap(voId.getVersion(), true);
        headMap.forEach((v, obj) -> {
            voId.setVersion(v);
            // this could cause excessive handleEviction calls
            objectCache.invalidate(voId);
        });
        int count = headMap.size();
        headMap.clear();

        return count;
    }

    /**
     * Get the soft reference to the versioned object (e.g. PersistentCorfuTable)
     * associated with the given key. Return null if the key does not exist in cache.
     *
     * @param voId the id of the versioned object
     * @return the versioned object, or null if not exist
     */
    public SoftReference<ICorfuSMR> get(VersionedObjectIdentifier voId) {
        return new SoftReference<>(objectCache.getIfPresent(voId));
    }

    public SoftReference<ICorfuSMR> put(VersionedObjectIdentifier voId, ICorfuSMR versionedObject) {
        objectCache.put(voId, versionedObject);

        objectMap.putIfAbsent(voId.getObjectId(), new TreeMap<>());
        objectMap.get(voId.getObjectId()).put(voId.getVersion(), versionedObject);
        return new SoftReference<>(versionedObject);
    }

    public Boolean containsKey(VersionedObjectIdentifier voId) {
        return objectCache.asMap().containsKey(voId);
    }

    public SoftReference<Map.Entry<Long, ICorfuSMR>> floorEntry(VersionedObjectIdentifier voId) {
        return new SoftReference<>(objectMap.get(voId.getObjectId()).floorEntry(voId.getVersion()));
    }

}

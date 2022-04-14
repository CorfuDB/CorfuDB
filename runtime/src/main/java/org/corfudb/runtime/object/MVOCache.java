package org.corfudb.runtime.object;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;

import java.lang.ref.SoftReference;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * MVOCache is the centralized container that holds the reference to
 * all MVOs. Overall it provides put and get APIs and manages the
 * Cache-related properties (LRU) under the hood.
 *
 */
@Slf4j
public class MVOCache<T extends ICorfuSMR<T>> {

    private final CorfuRuntime runtime;

    // A registry to keep track of all opened MVOs
    private final ConcurrentHashMap<UUID, MultiVersionObject<T>> allMVOs = new ConcurrentHashMap<>();

    // The objectCache holds the strong references to all versioned objects
    // key is basically a pair of (objectId, version)
    // value is the versioned object such as PersistentCorfuTable
    private final Cache<VersionedObjectIdentifier, T> objectCache;

    // This objectVersions is updated at two places
    // 1) put() which adds a new version to the objectVersions
    // 2) as a side effect of put(), some old versions are evicted from the cache
    // TODO: access/mutation to objectMap should be synchronized
    // Q: Can we use objectCache.asMap() instead of maintaining an external view?
    // A: No. Although asMap() returns a thread-safe weakly-consistent map, it is
    //    not a tree structure so it's very inefficient to implement headMap() and
    //    floorEntry() on top of it.
    private final ConcurrentHashMap<UUID, TreeSet<Long>> objectVersions = new ConcurrentHashMap<>();

    private final long DEAFULT_CACHE_EXPIRY_TIME_IN_SECONDS = 300;

    final ScheduledExecutorService mvoCacheSyncThread = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(true)
                    .setNameFormat("MVOCacheSyncThread")
                    .build());

    public MVOCache(CorfuRuntime corfuRuntime) {
        runtime = corfuRuntime;

        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        objectCache = cacheBuilder.maximumSize(runtime.getParameters().getMaxCacheEntries())
                .expireAfterAccess(DEAFULT_CACHE_EXPIRY_TIME_IN_SECONDS, TimeUnit.SECONDS)
                .expireAfterWrite(DEAFULT_CACHE_EXPIRY_TIME_IN_SECONDS, TimeUnit.SECONDS)
                .removalListener(this::handleEviction)
                .recordStats()
                .build();

        mvoCacheSyncThread.scheduleAtFixedRate(this::syncMVOCache,
                runtime.getParameters().getMvoAutoSyncPeriod().toMinutes(),
                runtime.getParameters().getMvoAutoSyncPeriod().toMinutes(),
                TimeUnit.MINUTES);
    }

    public void stopMVOCacheSync() {
        mvoCacheSyncThread.shutdownNow();
    }

    /**
     * Callback function which is triggered by every guava cache eviction.
     *
     * @param notification guava cache eviction notification
     */
    private void handleEviction(RemovalNotification<VersionedObjectIdentifier, Object> notification) {
        VersionedObjectIdentifier voId = notification.getKey();
        int evictCount = prefixEvict(voId);
        log.info("evicted {} versions for object {} which is older than version {}",
                evictCount, voId.getObjectId(), voId.getVersion());
    }

    /**
     * Evict all versions of the given object up to the target version.
     *
     * @param voId  the object and version to perform prefixEvict
     * @return number of versions that has been evicted
     */
    private int prefixEvict(VersionedObjectIdentifier voId) {
        TreeSet<Long> allVersionsOfThisObject = objectVersions.get(voId.getObjectId());
        int count;

        synchronized (allVersionsOfThisObject) {
            // Get the headset up to the given version. Exclude the given version
            // if it is the highest version in the set. This is to guarantee that
            // at least one version exist in the objectMap for any object
            Set<Long> headSet = allVersionsOfThisObject.headSet(voId.getVersion(),
                    voId.getVersion() != allVersionsOfThisObject.last());
            headSet.forEach(version -> {
                voId.setVersion(version);
                // this could cause excessive handleEviction calls
                objectCache.invalidate(voId);
            });
            count = headSet.size();
            headSet.clear();
        }

        return count;
    }

    /**
     * Get the soft reference to the versioned object (e.g. PersistentCorfuTable)
     * associated with the given key. Return null if the key does not exist in cache.
     *
     * @param voId the id of the versioned object
     * @return the versioned object, or null if not exist
     */
    public SoftReference<T> get(VersionedObjectIdentifier voId) {
        return new SoftReference<>(objectCache.getIfPresent(voId));
    }

    /**
     * Put the voId and versionedObject to objectCache, update objectVersions.
     *
     * @param voId the object and version to add to cache
     * @param versionedObject the versioned object to add
     */
    public void put(VersionedObjectIdentifier voId, T versionedObject) {
        TreeSet<Long> allVersionsOfThisObject = objectVersions.computeIfAbsent(
                voId.getObjectId(), k -> new TreeSet<>());
        synchronized (allVersionsOfThisObject) {
            objectCache.put(voId, versionedObject);
            allVersionsOfThisObject.add(voId.getVersion());
        }
    }

    /**
     * Returns true if the objectCache has the target voId.
     *
     * @param voId the object and version to check
     * @return true if target exists
     */
    public Boolean containsKey(VersionedObjectIdentifier voId) {
        return objectCache.asMap().containsKey(voId);
    }

    /**
     * For a given voId, find in all versions of this object that has the greatest
     * version && version <= voId.getVersion.
     *
     * @param voId the object and version to check
     * @return a pair of (voId, versionedObject) in which the voId contains the
     *         floor version.
     */
    public SoftReference<Map.Entry<VersionedObjectIdentifier, T>> floorEntry(VersionedObjectIdentifier voId) {
        final TreeSet<Long> allVersionsOfThisObject = objectVersions.get(voId.getObjectId());

        if (allVersionsOfThisObject == null) {
            // The object has not been created
            return new SoftReference<>(null);
        } else {
            synchronized (allVersionsOfThisObject) {
                Long floorVersion = allVersionsOfThisObject.floor(voId.getVersion());
                if (floorVersion == null)
                    return new SoftReference<>(null);

                VersionedObjectIdentifier id = new VersionedObjectIdentifier(voId.getObjectId(), floorVersion);
                return new SoftReference<>(Pair.of(id, objectCache.getIfPresent(id)));
            }
        }
    }

    /**
     * Returns true if objectCache has any versionedObject of a certain object.
     * Checks the objectVersions.
     *
     * @param objectId the object id to check
     * @return true if target object exists
     */
    public Boolean containsObject(UUID objectId) {
        // TODO: what if an object existing in objectCache but its version is not in objectVersions?
        // This can happen when MVOCache::put is interrupted
        return objectVersions.containsKey(objectId);
    }

    public void registerMVO(UUID objectId, MultiVersionObject<T> mvo) {
        allMVOs.computeIfAbsent(objectId, key -> mvo);
    }

    private void syncMVOCache() {
        TokenResponse streamTails = runtime.getSequencerView()
                .query(allMVOs.keySet().toArray(new UUID[0]));
        allMVOs.forEach((uuid, mvo) -> {
            if (objectVersions.get(uuid).last() < streamTails.getStreamTail(uuid)) {
                // Sync to the latest state
                mvo.getVersionedObjectUnderLock(streamTails.getStreamTail(uuid));
            }
        });
    }
}

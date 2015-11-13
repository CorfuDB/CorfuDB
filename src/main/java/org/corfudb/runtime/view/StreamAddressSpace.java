package org.corfudb.runtime.view;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.thrift.ReadCode;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.logunits.INewWriteOnceLogUnit;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is the default implementation of a stream address space, which is backed by a LRU cache.
 * Created by mwei on 8/26/15.
 */
@Slf4j
public class StreamAddressSpace implements IStreamAddressSpace {

    /**
     * The Corfu instance that this StreamAddressSpace serves.
     */
    @Getter
    final ICorfuDBInstance instance;

    /**
     * The cache that supports this stream address space.
     */
    @Getter
    AsyncLoadingCache<Long, StreamAddressSpaceEntry> cache;

    StreamAddressEntryCode fromLogUnitcode(INewWriteOnceLogUnit.ReadResultType rrt) {
        switch (rrt)
        {
            case DATA:
                return StreamAddressEntryCode.DATA;
            case FILLED_HOLE:
                return StreamAddressEntryCode.HOLE;
            case EMPTY:
                return StreamAddressEntryCode.EMPTY;
            case TRIMMED:
                return StreamAddressEntryCode.TRIMMED;
        }
        throw new RuntimeException("unknown read result + " + rrt.toString());
    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<StreamAddressSpaceEntry> load(long index)
    {
        log.trace("Load[{}]: Read requested", index);
        int chainNum = (int) (index % instance.getView().getSegments().get(0).getGroups().size());
        List<IServerProtocol> chain = instance.getView().getSegments().get(0).getGroups().get(chainNum);
        int unitNum = chain.size() - 1;
        INewWriteOnceLogUnit lu = (INewWriteOnceLogUnit) chain.get(unitNum);
        return lu.read(index)
                .exceptionally(e -> {
                    log.error(e.getMessage());
                    return null;
                })
                .thenApply(r -> {
                    if (r == null)
                    {
                        cache.synchronous().invalidate(index);
                        return null;
                    }
                    switch (r.getResult()) {
                        case DATA:
                            log.trace("Load[{}]: Data", index);
                            return new StreamAddressSpaceEntry(r.getStreams(), index, StreamAddressEntryCode.DATA, r.getPayload());
                        case EMPTY:
                            //self invalidate
                            log.trace("Load[{}]: Empty", index);
                            cache.synchronous().invalidate(index);
                            return null;
                        default:
                            log.trace("Load[{}]: {}", index , fromLogUnitcode(r.getResult()));
                            return new StreamAddressSpaceEntry(index, fromLogUnitcode(r.getResult()));
                    }
                });
    }

    /**
     * This constructor builds a default stream address space with a LRU cache of 10,000 entries.
     * @param instance     The Corfu instance that this StreamAddressSpace serves.
     */
    @SuppressWarnings("unchecked")
    public StreamAddressSpace(@NonNull ICorfuDBInstance instance)
    {
        this.instance = instance;
        cache = buildCache();
    }

    /** Build the asynchronous loading cache.
     *
     * @return A new instance of an async loading cache.
     */
    private AsyncLoadingCache<Long, StreamAddressSpaceEntry> buildCache()
    {
        AtomicInteger threadNum = new AtomicInteger();
        return Caffeine.newBuilder()
                .maximumSize(10_000)
                .executor(Executors.newFixedThreadPool(8, new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable runnable) {
                        Thread thread = Executors.defaultThreadFactory().newThread(runnable);
                        thread.setName("CachePool-" + threadNum.getAndIncrement());
                        thread.setDaemon(true);
                        return thread;
                    }
                }))
                .buildAsync(idx -> {
                    try {
                        return load(idx).get();
                    }
                    catch (Exception e)
                    {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * Asynchronously write to the stream address space.
     *
     * @param offset  The offset (global index) to write to.
     * @param streams The streams that this entry will belong to.
     * @param payload The unserialized payload that belongs to this entry.
     */
    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<StreamAddressWriteResult> writeAsync(long offset, Set<UUID> streams, Object payload) {
        int chainNum = (int) (offset % instance.getView().getSegments().get(0).getGroups().size());
        List<IServerProtocol> chain = instance.getView().getSegments().get(0).getGroups().get(chainNum);
        int unitNum = chain.size() - 1;
        INewWriteOnceLogUnit lu = (INewWriteOnceLogUnit) chain.get(unitNum);
        return lu.write(offset, streams, 0, payload)
                .thenApply(res -> {
                    if (res == INewWriteOnceLogUnit.WriteResult.OK) {
                        // Write was OK, so generate an entry in our cache and return OK.
                        StreamAddressSpaceEntry s = new StreamAddressSpaceEntry(streams, offset,
                                StreamAddressEntryCode.DATA, payload);
                        cache.put(offset, CompletableFuture.completedFuture(s));
                        log.trace("Write[{}] complete, cached.", offset);
                        return StreamAddressWriteResult.OK;
                    } else {
                        switch (res) {
                            case TRIMMED:
                                log.trace("Write[{}] FAILED, trimmed!", offset);
                                return StreamAddressWriteResult.TRIMMED;
                            case OVERWRITE:
                                log.trace("Write[{}] FAILED, overwrite!", offset);
                                return StreamAddressWriteResult.OVERWRITE;
                            default:
                                log.trace("Write[{}] FAILED, unknown ({})!", offset, res.name());
                                throw new RuntimeException("Unknown writeresult type: " + res.name());
                        }
                    }
                });
    }

    /**
     * Asynchronously read from the stream address space.
     *
     * @param offset The offset (global index) to read from.
     * @return A StreamAddressSpaceEntry containing the data that was read.
     */
    @Override
    public CompletableFuture<StreamAddressSpaceEntry> readAsync(long offset) {
        return cache.get(offset);
    }

    /**
     * Fill an address in the address space with a hole entry. This method is unreliable (not guaranteed to send a request
     * to any log unit) and asynchronous.
     *
     * @param offset The offset (global index) to fill.
     */
    @Override
    public void fillHole(long offset) {
        int chainNum = (int) (offset % instance.getView().getSegments().get(0).getGroups().size());

        //Next, we perform the write. We must write to every replica in the chain, in sequence.
        List<IServerProtocol> chain = instance.getView().getSegments().get(0).getGroups().get(chainNum);
        for (IServerProtocol p : chain) {
            INewWriteOnceLogUnit lu = (INewWriteOnceLogUnit) p;
            lu.fillHole(offset);
        }
    }

    /**
     * Trim a prefix of a stream.
     *
     * @param stream The ID of the stream to be trimmed.
     * @param prefix The prefix to be trimmed, inclusive.
     */
    @Override
    public void trim(UUID stream, long prefix) {
        // iterate through every log unit in the system
        // TODO: handle multiple segments.
        for (List<IServerProtocol> chain  : instance.getView().getSegments().get(0).getGroups()) {
            for (IServerProtocol p : chain) {
                INewWriteOnceLogUnit lu = (INewWriteOnceLogUnit) p;
                lu.trim(stream, prefix);
            }
        }
    }

    /**
     * Reset all caches.
     */
    @Override
    public void resetCaches() {
        /* Flush the async loading cache. */
        cache.synchronous().invalidateAll();
        log.info("Stream address space loading cache reset.");
        cache = buildCache();
    }


    StreamAddressEntryCode entryCodeFromReadCode(ReadCode code)
    {
        switch(code)
        {
            case READ_DATA:
                return StreamAddressEntryCode.DATA;
            case READ_EMPTY:
                return StreamAddressEntryCode.EMPTY;
            case READ_FILLEDHOLE:
                return StreamAddressEntryCode.HOLE;
            case READ_TRIMMED:
                return StreamAddressEntryCode.TRIMMED;
        }
        return null;
    }
}

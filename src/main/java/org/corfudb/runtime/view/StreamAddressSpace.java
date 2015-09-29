package org.corfudb.runtime.view;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.corfudb.infrastructure.thrift.ReadCode;
import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.OutOfSpaceException;
import org.corfudb.runtime.OverwriteException;
import org.corfudb.runtime.TrimmedException;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.logunits.INewWriteOnceLogUnit;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;

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
        int chainNum = (int) (index % instance.getView().getSegments().get(0).getGroups().size());
        List<IServerProtocol> chain = instance.getView().getSegments().get(0).getGroups().get(chainNum);
        int unitNum = chain.size() - 1;
        INewWriteOnceLogUnit lu = (INewWriteOnceLogUnit) chain.get(unitNum);
        return lu.read(index)
                .thenApply(r -> {
                   switch (r.getResult())
                   {
                       case DATA:
                           return new StreamAddressSpaceEntry(r.getStreams(), index, StreamAddressEntryCode.DATA, r.getPayload());
                       case EMPTY:
                           //self invalidate
                           cache.synchronous().invalidate(index);
                           return null;
                       default:
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
        cache = Caffeine.newBuilder()
                .maximumSize(10_000)
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
                        return StreamAddressWriteResult.OK;
                    } else {
                        switch (res) {
                            case TRIMMED:
                                return StreamAddressWriteResult.TRIMMED;
                            case OVERWRITE:
                                return StreamAddressWriteResult.OVERWRITE;
                            default:
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

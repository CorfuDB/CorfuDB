package org.corfudb.runtime.view;

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
    LoadingCache<Long, StreamAddressSpaceEntry> cache;

    /**
     * This constructor builds a default stream address space with a LRU cache of 10,000 entries.
     * @param instance     The Corfu instance that this StreamAddressSpace serves.
     */
    @SuppressWarnings("unchecked")
    public StreamAddressSpace(@NonNull ICorfuDBInstance instance)
    {
        this.instance = instance;
        cache = Caffeine.newBuilder()
                .weakKeys()
                .maximumSize(10_000)
                .build(index -> {
                    //TODO: fold Amy's replication protocol into this..., for now we only support chain replication.
                    //In addition, we currently only support the new-style logging units. Any old style logging units
                    //in the system will cause this class to fail.
                    return IRetry.build(ExponentialBackoffRetry.class, () -> {
                        //First, we determine which chain to use.
                        int chainNum = (int) (index % instance.getView().getSegments().get(0).getGroups().size());

                        //Next, we perform the read. Currently we read from the last unit in the chain, but once we
                        //have a mechanism that allows us to determine the tail, we can spread those reads
                        //across the chain.
                        List<IServerProtocol> chain = instance.getView().getSegments().get(0).getGroups().get(chainNum);
                        int unitNum = chain.size() - 1;
                        INewWriteOnceLogUnit lu = (INewWriteOnceLogUnit) chain.get(unitNum);
                        INewWriteOnceLogUnit.WriteOnceLogUnitRead result = lu.read(index / instance.getView().getSegments().get(0).getGroups().size());

                        if (result.getResult() == ReadCode.READ_EMPTY) {
                            return new StreamAddressSpaceEntry<>(null, null, index, StreamAddressEntryCode.EMPTY);
                        }
                        else if (result.getResult() == ReadCode.READ_FILLEDHOLE)
                        {
                            return new StreamAddressSpaceEntry<>(null, null, index, StreamAddressEntryCode.HOLE);
                        }
                        else if (result.getResult() == ReadCode.READ_DATA) {
                            return new StreamAddressSpaceEntry<>(result.getStreams(), result.getData(), index, StreamAddressEntryCode.DATA);
                        }

                        return new StreamAddressSpaceEntry<>(null, null, index, entryCodeFromReadCode(result.getResult()));
                    })
                            .onException(NetworkException.class, e -> {
                                log.error("Error performing read, requesting reconfiguration and retry...");
                                instance.getConfigurationMaster().requestReconfiguration(e);
                                return true;
                            })
                            .run();
                });
    }

    /**
     * Write to the stream address space.
     *
     * @param offset  The offset (global index) to write to.
     * @param streams The streams that this entry will belong to.
     * @param payload The payload that belongs to this entry.
     * @throws OverwriteException  If the index has been already written to.
     * @throws TrimmedException    If the index has been previously written to and is now released for garbage collection.
     * @throws OutOfSpaceException If there is no space remaining in the current view of the address space.
     */
    @Override
    public void write(long offset, Set<UUID> streams, ByteBuffer payload) throws OverwriteException, TrimmedException, OutOfSpaceException {
        IRetry.build(ExponentialBackoffRetry.class, OverwriteException.class, TrimmedException.class, OutOfSpaceException.class, () -> {
            //First, we determine which chain to use.
            int chainNum = (int) (offset % instance.getView().getSegments().get(0).getGroups().size());

            //Next, we perform the write. We must write to every replica in the chain, in sequence.
            List<IServerProtocol> chain = instance.getView().getSegments().get(0).getGroups().get(chainNum);
            for (IServerProtocol p : chain) {
                INewWriteOnceLogUnit lu = (INewWriteOnceLogUnit) p;
                lu.write(offset, streams, payload);
            }

            //finally, put this entry in the cache so we don't need to go over the network.
            cache.put(offset, new StreamAddressSpaceEntry<>(streams, payload, offset, StreamAddressEntryCode.DATA));
            return true;
        }).onException(NetworkException.class, e -> {
            log.error("Error performing read, requesting reconfiguration and retry...");
            instance.getConfigurationMaster().requestReconfiguration(e);
            return true;
        })
            .run();
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
     * Read from the stream address space.
     *
     * @param offset The offset (global index) to read from.
     * @return A StreamAddressSpaceEntry which represents this entry, or null, if there is no entry at this space.
     * @throws TrimmedException If the index has been previously written to and is now released for garbage collection.
     */
    @Override
    public StreamAddressSpaceEntry read(long offset) throws TrimmedException {
        StreamAddressSpaceEntry entry = cache.get(offset);
        if (entry.getCode() == StreamAddressEntryCode.EMPTY) { cache.invalidate(offset); }
        return entry;
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

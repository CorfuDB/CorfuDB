/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.StreamCOWEntry;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.exceptions.OverwriteException;

import java.util.*;

/**
 * Helper object for StreamView with the methods specific for replex replication mode.
 * Created by Konstantin Spirov on 1/30/2017.
 */
@Slf4j
class ReplexReplicationStreamViewDelegate implements IStreamViewDelegate {

    private ReplexReplicationStreamViewDelegate() {
    }

    @Override
    public ILogData read(StreamView v, long maxGlobal) {
        while (true) {
            long thisRead = v.getCurrentContext().logPointer.get();
            if (thisRead > maxGlobal) {
                return null;
            }
            log.trace("Doing a stream read, stream: {}, address: {}", v.streamID, thisRead);
            LogData result = v.runtime.getAddressSpaceView().read(v.streamID, thisRead, 1L).get(thisRead);
            v.getCurrentContext().logPointer.incrementAndGet();

            if (result.getType() == DataType.EMPTY) {
                //determine whether or not this is a hole
                long latestToken = v.runtime.getSequencerView().nextToken(Collections.singleton(v.streamID), 0)
                        .getStreamAddresses().get(v.streamID);
                log.trace("Read[{}]: latest token at {}", v.streamID, latestToken);
                if (latestToken < thisRead) {
                    v.getCurrentContext().logPointer.decrementAndGet();
                    return null;
                }
                log.debug("Read[{}]: hole detected at {} (token at {}), attempting fill.", v.streamID, thisRead, latestToken);
                try {
                    v.runtime.getAddressSpaceView().fillStreamHole(v.streamID, thisRead);
                } catch (OverwriteException oe) {
                    //ignore overwrite.
                }
                result = v.runtime.getAddressSpaceView().read(v.streamID, thisRead, 1L).get(thisRead);
                log.debug("Read[{}]: holeFill {} result: {}", v.streamID, thisRead, result.getType());
            }
            if (result.getMetadataMap().get(IMetadata.LogUnitMetadataType.STREAM_ADDRESSES) == null)
                continue;

            Set<UUID> streams = ((Map<UUID, Long>) result.getMetadataMap().get(IMetadata.LogUnitMetadataType.STREAM_ADDRESSES)).keySet();
            if (streams != null && streams.contains(v.getCurrentContext().contextID)) {
                log.trace("Read[{}]: valid entry at {}", v.streamID, thisRead);
                Object res = result.getPayload(v.runtime);
                if (res instanceof StreamCOWEntry) {
                    StreamCOWEntry ce = (StreamCOWEntry) res;
                    log.trace("Read[{}]: encountered COW entry for {}@{}", v.streamID, ce.getOriginalStream(),
                            ce.getFollowUntil());
                    v.streamContexts.add(v.new StreamContext(ce.getOriginalStream(), ce.getFollowUntil()));
                } else {
                    return result;
                }
            }
        }
    }

    public ILogData[] readTo(StreamView v, long pos) {
        long latestToken = pos;

        latestToken = v.runtime.getSequencerView().nextToken(Collections.singleton(v.streamID), 0)
                .getStreamAddresses().get(v.streamID);
        log.trace("Linearization point set to {}", latestToken);

        if (latestToken < v.getCurrentContext().logPointer.get())
            return (new ArrayList<LogData>()).toArray(new ILogData[0]);

        //if (getCurrentContext().logPointer.get() != 0 && latestToken == getCurrentContext().logPointer.get())
        //    return (new ArrayList<LogData>()).toArray(new LogData[0]);
        // We can do a bulk read
        Map<Long, LogData> readResult = v.runtime.getAddressSpaceView().read(v.streamID, v.getCurrentContext().logPointer.get(),
                latestToken - v.getCurrentContext().logPointer.get() + 1);
        v.getCurrentContext().logPointer.addAndGet(latestToken - v.getCurrentContext().logPointer.get() + 1);
        ArrayList<LogData> al = new ArrayList<>();
        for (Long addr : readResult.keySet()) {
            // Now we effectively copy the logic from the read() function above.
            if (readResult.get(addr) == null) {
                continue;
            }

            if (readResult.get(addr).getType() == DataType.EMPTY) {
                if (addr <= latestToken) {
                    // If it's a hole, fill it and don't return it
                    LogData retry;
                    while (true) {
                        log.debug("Replex readTO[{}]: hole detected at {} (token at {}), attempting fill.", v.streamID, addr, latestToken);
                        try {
                            v.runtime.getAddressSpaceView().fillStreamHole(v.streamID, addr);
                        } catch (OverwriteException oe) {
                            //ignore overwrite.
                        }
                        retry = v.runtime.getAddressSpaceView().read(v.streamID, addr, 1L).get(addr);
                        if (retry.getType() != DataType.EMPTY)
                            break;
                    }
                    if (retry.getMetadataMap().get(IMetadata.LogUnitMetadataType.STREAM_ADDRESSES) == null)
                        continue;
                    Set<UUID> streams = ((Map<UUID, Long>) retry.getMetadataMap().get(IMetadata.LogUnitMetadataType.STREAM_ADDRESSES)).keySet();
                    if (streams != null && streams.contains(v.getCurrentContext().contextID)) {
                        log.trace("Read[{}]: valid entry at {}", v.streamID, retry);
                        Object res = retry.getPayload(v.runtime);
                        if (res instanceof StreamCOWEntry) {
                            StreamCOWEntry ce = (StreamCOWEntry) res;
                            log.trace("Read[{}]: encountered COW entry for {}@{}", v.streamID, ce.getOriginalStream(),
                                    ce.getFollowUntil());
                            v.streamContexts.add(v.new StreamContext(ce.getOriginalStream(), ce.getFollowUntil()));
                        } else {
                            al.add(retry);
                        }
                    }
                } else {
                    v.getCurrentContext().logPointer.decrementAndGet();
                }
                continue;
            }
            al.add(readResult.get(addr));
        }
        return al.toArray(new LogData[al.size()]);
    }

    static class DelegateHolder {
        static final ReplexReplicationStreamViewDelegate delegate = new ReplexReplicationStreamViewDelegate();
    }
}

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
 * Helper object for StreamView with the methods specific for chain replication.
 * Created by Konstantin Spirov on 1/30/2017.
 */
@Slf4j
class ChainReplicationStreamViewDelegate implements IStreamViewDelegate {

    ChainReplicationStreamViewDelegate() {
    }

    @Override
    public ILogData read(StreamView v, long maxGlobal) {
        while (true) {
            if (v.getCurrentContext().logPointer.get() > v.getCurrentContext().maxAddress) {
                StreamView.StreamContext last = v.streamContexts.pollFirst();
                log.trace("Completed context {}@{}, removing.", last.contextID, last.maxAddress);
            }
            Long thisRead = v.getCurrentContext().currentBackpointerList.pollFirst();
            if (thisRead == null) {
                v.getCurrentContext().currentBackpointerList =
                        v.resolveBackpointersToRead(v.getCurrentContext().contextID, v.getCurrentContext().logPointer.get());
                log.trace("Backpointer list was empty, it has been filled with {} entries.",
                        v.getCurrentContext().currentBackpointerList.size());
                if (v.getCurrentContext().currentBackpointerList.size() == 0) {
                    log.trace("No backpointers resolved, nothing to read.");
                    return null;
                }
                thisRead = v.getCurrentContext().currentBackpointerList.pollFirst();
            }
            if (thisRead > maxGlobal) {
                v.getCurrentContext().currentBackpointerList.add(thisRead);
                return null;
            }
            v.getCurrentContext().logPointer.set(thisRead + 1);
            log.trace("Read[{}]: reading at {}", v.streamID, thisRead);
            ILogData r = v.runtime.getAddressSpaceView().read(thisRead);
            if (r.getType() == DataType.EMPTY) {
                //determine whether or not this is a hole
                long latestToken = v.runtime.getSequencerView().nextToken(Collections.singleton(v.streamID), 0).getToken();
                log.trace("Read[{}]: latest token at {}", v.streamID, latestToken);
                if (latestToken < thisRead) {
                    v.getCurrentContext().logPointer.decrementAndGet();
                    return null;
                }
                log.debug("Read[{}]: hole detected at {} (token at {}), attempting fill.", v.streamID, thisRead, latestToken);
                try {
                    v.runtime.getAddressSpaceView().fillHole(thisRead);
                } catch (OverwriteException oe) {
                    //ignore overwrite.
                }
                r = v.runtime.getAddressSpaceView().read(thisRead);
                log.debug("Read[{}]: holeFill {} result: {}", v.streamID, thisRead, r.getType());
            }
            Set<UUID> streams = (Set<UUID>) r.getMetadataMap().get(IMetadata.LogUnitMetadataType.STREAM);
            if (streams != null && streams.contains(v.getCurrentContext().contextID)) {
                log.trace("Read[{}]: valid entry at {}", v.streamID, thisRead);
                Object res = r.getPayload(v.runtime);
                if (res instanceof StreamCOWEntry) {
                    StreamCOWEntry ce = (StreamCOWEntry) res;
                    log.trace("Read[{}]: encountered COW entry for {}@{}", v.streamID, ce.getOriginalStream(),
                            ce.getFollowUntil());
                    v.streamContexts.add(v.new StreamContext(ce.getOriginalStream(), ce.getFollowUntil()));
                } else {
                    return r;
                }
            }
        }
    }

    @Override
    public ILogData[] readTo(StreamView v, long pos) {
        long latestToken = pos;
        boolean max = false;
        if (pos == Long.MAX_VALUE) {
            max = true;
            latestToken = v.runtime.getSequencerView().nextToken(Collections.singleton(v.streamID), 0).getToken();
            log.trace("Linearization point set to {}", latestToken);
        }
        ArrayList<ILogData> al = new ArrayList<>();
        log.debug("Stream[{}] pointer[{}], readTo {}", v.streamID, v.getCurrentContext().logPointer.get(), latestToken);
        while (v.getCurrentContext().logPointer.get() <= latestToken) {
            ILogData r = read(v, pos);
            if (r != null && (max || r.getGlobalAddress() <= pos)) {
                al.add(r);
            } else {
                break;
            }
        }
        return al.toArray(new ILogData[al.size()]);
    }

    static class DelegateHolder {
        public static final ChainReplicationStreamViewDelegate delegate = new ChainReplicationStreamViewDelegate();
    }

}

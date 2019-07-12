package org.corfudb.runtime;

import lombok.Getter;
import org.corfudb.protocols.logprotocol.*;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class GarbageInformer {

    @Getter
    Map<Long, SMRGarbageEntry> addressToGarbage = new ConcurrentHashMap<>();

    public synchronized void add(long markerAddress, SMRRecordLocator locator) {
        SMRGarbageEntry garbageInfo = generateGarbageInfo(markerAddress, locator);
        long globalAddress = locator.getGlobalAddress();
        if (!addressToGarbage.containsKey(globalAddress)) {
            addressToGarbage.put(globalAddress, garbageInfo);
        } else {
            addressToGarbage.get(globalAddress).merge(garbageInfo);
        }
    }

    private SMRGarbageEntry generateGarbageInfo(long markerAddress, SMRRecordLocator locator) {
        int serializedSize = locator.getSerializedSize();

        SMRGarbageRecord garbageRecord = new SMRGarbageRecord(markerAddress, serializedSize);
        SMRGarbageEntry garbageEntry = new SMRGarbageEntry();
        garbageEntry.add(locator.getStreamId(), locator.getIndex(), garbageRecord);

        return garbageEntry;
    }
}

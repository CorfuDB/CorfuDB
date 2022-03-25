package org.corfudb.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.StreamAddressSpace;

import java.util.*;

@Slf4j
public class DynamicTriggerPolicy implements ICompactionTriggerPolicy{

    private final List<UUID> sensitiveStreams = new ArrayList<>();
    private final CorfuRuntime corfuRuntime;
    private Map<UUID, Long> previousSensitiveStreamsSize = new HashMap<>();
    private Long previousAddressSpaceSize = Long.MAX_VALUE;
    private Long previousTriggerMillis = Long.valueOf(0);

    DynamicTriggerPolicy(CorfuRuntime corfuRuntime, List<TableName> sensitiveTables) {
        this.corfuRuntime = corfuRuntime;

        for (TableName table : sensitiveTables) {
            sensitiveStreams.add(CorfuRuntime.getStreamID(
                    TableRegistry.getFullyQualifiedTableName(table.getNamespace(), table.getTableName())));
        }
    }

    @Override
    public boolean shouldTrigger(long interval) {
        //1. Checks if one of the sensitive streams requires compaction
        //2. Checks if the address space size is bigger than previous cycle
        //3. If it has been > 30 mins since the last compaction

        log.info("running shouldTrigger");
        Long currentAddressSpaceSize = corfuRuntime.getAddressSpaceView().getLogTail()
                - corfuRuntime.getAddressSpaceView().getTrimMark().getSequence();
        boolean shouldTrigger = false;
        for (UUID streamId : sensitiveStreams) {
            StreamAddressSpace streamAddressSpace = corfuRuntime.getSequencerView()
                    .getStreamAddressSpace(new StreamAddressRange(streamId, Address.MAX, Address.NON_ADDRESS));

            Long previousSize = (!previousSensitiveStreamsSize.containsKey(streamId)) ? Long.MAX_VALUE :
                    previousSensitiveStreamsSize.get(streamId);
            if (streamAddressSpace.size() > previousSize) {
                shouldTrigger = true;
            }
        }

        if (currentAddressSpaceSize > previousAddressSpaceSize ||
                System.currentTimeMillis() - previousTriggerMillis >= interval) {
            shouldTrigger = true;
        }

        if (shouldTrigger) {
             setPreviousSensitiveStreamsSize();
             previousAddressSpaceSize = currentAddressSpaceSize;
             previousTriggerMillis = System.currentTimeMillis();
         }

        log.info("shouldTrigger: {}", shouldTrigger);
        return shouldTrigger;
    }

    private void setPreviousSensitiveStreamsSize() {
        for (UUID streamId : sensitiveStreams) {
            StreamAddressSpace streamAddressSpace = corfuRuntime.getSequencerView()
                    .getStreamAddressSpace(new StreamAddressRange(streamId, Address.MAX, Address.NON_ADDRESS));
            previousSensitiveStreamsSize.put(streamId, streamAddressSpace.size());
        }
    }
}

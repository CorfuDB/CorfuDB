package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.UUID;

/**
 * Created by mwei on 12/11/15.
 */
@Slf4j
public class ChainReplicationView extends AbstractReplicationView {

    public ChainReplicationView(Layout l)
    {
        super(l);
    }

    /**
     * Write the given object to an address and streams, using the replication method given.
     *
     * @param address An address to write to.
     * @param stream  The streams which will belong on this entry.
     * @param data    The data to write.
     */
    @Override
    public void write(long address, Set<UUID> stream, Object data)
    throws Exception {
        int numUnits = getLayout().getSegmentLength(address);
        for (int i = 0; i < numUnits; i++)
        {
            log.trace("Write[{}]: chain {}/{}", address, i, numUnits);
            getLayout().getLogUnitClient(address, i)
                    .write(address, stream, 0L, data).get();
        }
    }
}

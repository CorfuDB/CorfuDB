package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg.ReadResult;

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
            log.trace("Write[{}]: chain {}/{}", address, i+1, numUnits);
            // In chain replication, we write synchronously to every unit in the chain.
            getLayout().getLogUnitClient(address, i)
                    .write(address, stream, 0L, data).get();
        }
    }

    /**
     * Read the given object from an address, using the replication method given.
     *
     * @param address The address to read from.
     * @return The result of the read.
     */
    @Override
    public ReadResult read(long address) throws Exception {
        int numUnits = getLayout().getSegmentLength(address);
        log.trace("Read[{}]: chain {}/{}", address, numUnits, numUnits);
        // In chain replication, we read from the last unit, though we can optimize if we
        // know where the committed tail is.
        return getLayout().getLogUnitClient(address, numUnits-1).read(address).get();
    }

    /**
     * Fill a hole at an address, using the replication method given.
     *
     * @param address The address to hole fill at.
     */
    @Override
    public void fillHole(long address) throws Exception {
        int numUnits = getLayout().getSegmentLength(address);
        for (int i = 0; i < numUnits; i++)
        {
            log.trace("fillHole[{}]: chain {}/{}", address, i+1, numUnits);
            // In chain replication, we write synchronously to every unit in the chain.
            getLayout().getLogUnitClient(address, i)
                    .fillHole(address).get();
        }
    }
}

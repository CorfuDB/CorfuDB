package org.corfudb.runtime.view;

import com.google.common.util.concurrent.Uninterruptibles;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg.ReadResult;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.util.CFUtils;

import java.util.Set;
import java.util.UUID;

/** A view of an address implemented by chain replication.
 *
 * Chain replication is a protocol defined by Renesse and Schneider (OSDI'04),
 * which is an alternative to quorum based replication protocols. It requires
 * that writes be written to a chain of replicas in order, and that reads are
 * always performed at the end of the chain.
 *
 * The primary advantage of chain replication is that unlike quorum replication,
 * where reads must contact a quorum of replicas, chain replication only requires
 * contacting the last replica in the chain. However, writes require contacting
 * every replica in sequence. In general, chain replication is best suited for
 * small chains.
 *
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
    throws OverwriteException {
        int numUnits = getLayout().getSegmentLength(address);
        for (int i = 0; i < numUnits; i++)
        {
            log.trace("Write[{}]: chain {}/{}", address, i+1, numUnits);
            // In chain replication, we write synchronously to every unit in the chain.
                CFUtils.getUninterruptibly(
                        getLayout().getLogUnitClient(address, i)
                                .write(address, stream, 0L, data), OverwriteException.class);
        }
    }

    /**
     * Read the given object from an address, using the replication method given.
     *
     * @param address The address to read from.
     * @return The result of the read.
     */
    @Override
    public ReadResult read(long address) {
        int numUnits = getLayout().getSegmentLength(address);
        log.trace("Read[{}]: chain {}/{}", address, numUnits, numUnits);
        // In chain replication, we read from the last unit, though we can optimize if we
        // know where the committed tail is.
        return new ReadResult(address,
                CFUtils.getUninterruptibly(getLayout().getLogUnitClient(address, numUnits - 1).read(address)));
    }

    /**
     * Fill a hole at an address, using the replication method given.
     *
     * @param address The address to hole fill at.
     */
    @Override
    public void fillHole(long address) throws OverwriteException {
        int numUnits = getLayout().getSegmentLength(address);
        for (int i = 0; i < numUnits; i++)
        {
            log.trace("fillHole[{}]: chain {}/{}", address, i+1, numUnits);
            // In chain replication, we write synchronously to every unit in the chain.
            CFUtils.getUninterruptibly(getLayout().getLogUnitClient(address, i)
                    .fillHole(address), OverwriteException.class);
        }
    }
}

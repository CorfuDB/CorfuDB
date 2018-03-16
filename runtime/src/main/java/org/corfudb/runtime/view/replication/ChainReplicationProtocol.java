package org.corfudb.runtime.view.replication;

import com.google.common.collect.Range;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.RecoveryException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;



/**
 * Created by mwei on 4/6/17.
 */
@Slf4j
public class ChainReplicationProtocol extends AbstractReplicationProtocol {

    public ChainReplicationProtocol(IHoleFillPolicy holeFillPolicy) {
        super(holeFillPolicy);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(Layout layout, ILogData data) throws OverwriteException {
        final long globalAddress = data.getGlobalAddress();
        int numUnits = layout.getSegmentLength(globalAddress);

        // To reduce the overhead of serialization, we serialize only the
        // first time we write, saving when we go down the chain.
        try (ILogData.SerializationHandle sh =
                     data.getSerializedForm()) {
            log.trace("Write[{}]: chain head {}/{}", globalAddress, 1, numUnits);
            // In chain replication, we start at the chain head.
            try {
                CFUtils.getUninterruptibly(
                        layout.getLogUnitClient(globalAddress, 0)
                                .write(sh.getSerialized()),
                        OverwriteException.class);
                propagate(layout, globalAddress, sh.getSerialized());
            } catch (OverwriteException oe) {
                // Some other wrote here (usually due to hole fill)
                // We need to invoke the recovery protocol, in case
                // the write wasn't driven to completion.
                recover(layout, globalAddress);
                throw oe;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ILogData peek(Layout layout, long globalAddress) {
        int numUnits = layout.getSegmentLength(globalAddress);
        log.trace("Read[{}]: chain {}/{}", globalAddress, numUnits, numUnits);
        // In chain replication, we read from the last unit, though we can optimize if we
        // know where the committed tail is.
        ILogData ret =  CFUtils.getUninterruptibly(layout
                .getLogUnitClient(globalAddress, numUnits - 1)
                                    .read(globalAddress)).getAddresses()
                .getOrDefault(globalAddress, null);
        return ret == null || ret.isEmpty() ? null : ret;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Long, ILogData> readAll(Layout layout, List<Long> globalAddresses) {
        long startAddress = globalAddresses.iterator().next();
        int numUnits = layout.getSegmentLength(startAddress);
        log.trace("readAll[{}]: chain {}/{}", globalAddresses, numUnits, numUnits);

        Map<Long, LogData> logResult = CFUtils.getUninterruptibly(layout
                .getLogUnitClient(startAddress, numUnits - 1)
                .read(globalAddresses)).getAddresses();

        //in case of a hole, do a normal read and use its hole fill policy
        Map<Long, ILogData> returnResult = new TreeMap<>();
        for (Map.Entry<Long, LogData> entry : logResult.entrySet()) {
            ILogData value = entry.getValue();
            if (value == null || value.isEmpty()) {
                value = read(layout, entry.getKey());
            }

            returnResult.put(entry.getKey(), value);
        }

        return returnResult;
    }

    @Override
    public Map<Long, ILogData> readRange(Layout layout, Set<Long> globalAddresses) {
        Range<Long> range = Range.encloseAll(globalAddresses);
        long startAddress = range.lowerEndpoint();
        long endAddress = range.upperEndpoint();
        int numUnits = layout.getSegmentLength(startAddress);
        log.trace("readRange[{}-{}]: chain {}/{}", startAddress, endAddress, numUnits, numUnits);

        Map<Long, LogData> logResult = CFUtils.getUninterruptibly(layout
                .getLogUnitClient(startAddress, numUnits - 1)
                .read(range)).getAddresses();

        //in case of a hole, do a normal read and use its hole fill policy
        Map<Long, ILogData> returnResult = new TreeMap<>();
        for (Map.Entry<Long, LogData> entry: logResult.entrySet()){
            ILogData value = entry.getValue();
            if (value == null || value.isEmpty()){
                value = read(layout, entry.getKey());
            }

            returnResult.put(entry.getKey(), value);
        }

        return returnResult;
    }

    /** Propagate a write down the chain, ignoring
     * any overwrite errors. It is expected that the
     * write has already successfully completed at
     * the head of the chain.
     *
     * @param layout        The layout to use for propagation.
     * @param globalAddress The global address to start
     *                      writing at.
     * @param data          The data to propagate, or NULL,
     *                      if it is to be a hole.
     */
    protected void propagate(Layout layout, long globalAddress, @Nullable ILogData data) {
        int numUnits = layout.getSegmentLength(globalAddress);

        for (int i = 1; i < numUnits; i++) {
            log.trace("Propogate[{}]: chain {}/{}", globalAddress, i + 1, numUnits);
            // In chain replication, we write synchronously to every unit
            // in the chain.
            try {
                if (data != null) {
                    CFUtils.getUninterruptibly(
                            layout.getLogUnitClient(globalAddress, i)
                                    .write(data),
                            OverwriteException.class);
                } else {
                    CFUtils.getUninterruptibly(layout.getLogUnitClient(globalAddress, i)
                            .fillHole(globalAddress), OverwriteException.class);
                }
            } catch (OverwriteException oe) {
                log.trace("Propogate[{}]: Completed by other writer", globalAddress);
            }
        }
    }

    /** Recover a failed write at the given global address,
     * driving it to completion by invoking the recovery
     * protocol.
     *
     * <p>When this function returns the given globalAddress
     * is guaranteed to contain a committed value.
     *
     * <p>If there was no data previously written at the address,
     * this function will throw a runtime exception. The
     * recovery protocol should -only- be invoked if we
     * previously were overwritten.
     *
     * @oaram layout            The layout to use for the recovery.
     * @param globalAddress     The global address to drive
     *                          the recovery protocol
     *
     */
    protected void recover(Layout layout, long globalAddress) {
        // In chain replication, we started writing from the head,
        // and propagated down to the tail. To recover, we start
        // reading from the head, which should have the data
        // we are trying to recover
        int numUnits = layout.getSegmentLength(globalAddress);
        log.debug("Recover[{}]: read chain head {}/{}", globalAddress, 1, numUnits);
        ILogData ld = CFUtils.getUninterruptibly(layout.getLogUnitClient(globalAddress, 0)
                .read(globalAddress)).getAddresses().getOrDefault(globalAddress, null);
        // If nothing was at the head, this is a bug and we
        // should fail with a runtime exception, as there
        // was nothing to recover - if the head was removed
        // due to a reconfiguration, a network exception
        // would have been thrown and the client should have
        // retried it's operation (in this case of a write,
        // it should have read to determine whether the
        // write was successful or not.
        if (ld == null || ld.isEmpty()) {
            throw new RecoveryException("Failed to read data during recovery at chain head.");
        }
        // now we go down the chain and write, ignoring any overwrite exception we get.
        for (int i = 1; i < numUnits; i++) {
            log.debug("Recover[{}]: write chain {}/{}", layout, i + 1, numUnits);
            // In chain replication, we write synchronously to every unit
            // in the chain.
            try {
                CFUtils.getUninterruptibly(
                        layout.getLogUnitClient(globalAddress, i)
                                .write(ld),
                        OverwriteException.class);
                // We successfully recovered a write to this member of the chain
                log.debug("Recover[{}]: recovered write at chain {}/{}", layout, i + 1, numUnits);
            } catch (OverwriteException oe) {
                // This member already had this data (in some cases, the write might have
                // been committed to all members, so this is normal).
                log.debug("Recover[{}]: overwritten at chain {}/{}", layout, i + 1, numUnits);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void holeFill(Layout layout, long globalAddress) {
        int numUnits = layout.getSegmentLength(globalAddress);
        log.trace("fillHole[{}]: chain head {}/{}", globalAddress, 1, numUnits);
        // In chain replication, we write synchronously to every unit in
        // the chain.
        try {
            CFUtils.getUninterruptibly(layout.getLogUnitClient(globalAddress, 0)
                    .fillHole(globalAddress), OverwriteException.class);
            propagate(layout, globalAddress, null);
        } catch (OverwriteException oe) {
            // The hole-fill failed. We must ensure the other writer's
            // value is adopted before returning.
            recover(layout, globalAddress);
        }
    }
}

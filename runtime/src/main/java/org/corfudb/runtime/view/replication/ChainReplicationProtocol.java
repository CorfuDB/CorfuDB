package org.corfudb.runtime.view.replication;

import com.google.common.collect.Range;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LayoutPrepareResponse;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.LogicalSequenceNumber;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TouchResponse;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.RecoveryException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.RuntimeLayout;
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
    public void write(RuntimeLayout runtimeLayout, ILogData data) throws OverwriteException {
        final LogicalSequenceNumber globalAddress = data.getGlobalAddress();
        int numUnits = runtimeLayout.getLayout().getSegmentLength(globalAddress);

        // To reduce the overhead of serialization, we serialize only the
        // first time we write, saving when we go down the chain.
        try (ILogData.SerializationHandle sh =
                     data.getSerializedForm()) {
            log.trace("Write[{}]: chain head {}/{}", globalAddress, 1, numUnits);
            // In chain replication, we start at the chain head.
            try {
                CFUtils.getUninterruptibly(
                        runtimeLayout.getLogUnitClient(globalAddress, 0)
                                .write(sh.getSerialized()),
                        OverwriteException.class);
                propagate(runtimeLayout, globalAddress, sh.getSerialized());
            } catch (OverwriteException oe) {
                // Some other wrote here (usually due to hole fill)
                // We need to invoke the recovery protocol, in case
                // the write wasn't driven to completion.
                recover(runtimeLayout, globalAddress);
                throw oe;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ILogData peek(RuntimeLayout runtimeLayout, LogicalSequenceNumber globalAddress) {
        int numUnits = runtimeLayout.getLayout().getSegmentLength(globalAddress);
        log.trace("Read[{}]: chain {}/{}", globalAddress, numUnits, numUnits);
        // In chain replication, we read from the last unit, though we can optimize if we
        // know where the committed tail is.
        ILogData ret = CFUtils.getUninterruptibly(
                runtimeLayout
                        .getLogUnitClient(globalAddress, numUnits - 1)
                        .read(globalAddress)).getAddresses()
                .getOrDefault(globalAddress, null);
        return ret == null || ret.isEmpty() ? null : ret;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<LogicalSequenceNumber, ILogData> readAll(RuntimeLayout runtimeLayout, List<LogicalSequenceNumber> globalAddresses) {
        LogicalSequenceNumber startAddress = globalAddresses.iterator().next();
        int numUnits = runtimeLayout.getLayout().getSegmentLength(startAddress);
        log.trace("readAll[{}]: chain {}/{}", globalAddresses, numUnits, numUnits);

        Map<LogicalSequenceNumber, LogData> logResult = CFUtils.getUninterruptibly(
                runtimeLayout
                        .getLogUnitClient(startAddress, numUnits - 1)
                        .read(globalAddresses)).getAddresses();

        //in case of a hole, do a normal read and use its hole fill policy
        Map<LogicalSequenceNumber, ILogData> returnResult = new TreeMap<>();
        for (Map.Entry<LogicalSequenceNumber, LogData> entry : logResult.entrySet()) {
            ILogData value = entry.getValue();
            if (value == null || value.isEmpty()) {
                value = read(runtimeLayout, entry.getKey());
            }

            returnResult.put(entry.getKey(), value);
        }

        return returnResult;
    }

    @Override
    public Map<LogicalSequenceNumber, ILogData> readRange(RuntimeLayout runtimeLayout, Set<LogicalSequenceNumber> globalAddresses) {
        Range<LogicalSequenceNumber> range = Range.encloseAll(globalAddresses);
        LogicalSequenceNumber startAddress = range.lowerEndpoint();
        LogicalSequenceNumber endAddress = range.upperEndpoint();
        int numUnits = runtimeLayout.getLayout().getSegmentLength(startAddress);
        log.trace("readRange[{}-{}]: chain {}/{}", startAddress, endAddress, numUnits, numUnits);

        Map<LogicalSequenceNumber, LogData> logResult = CFUtils.getUninterruptibly(
                runtimeLayout
                        .getLogUnitClient(startAddress, numUnits - 1)
                        .read(range)).getAddresses();

        //in case of a hole, do a normal read and use its hole fill policy
        Map<LogicalSequenceNumber, ILogData> returnResult = new TreeMap<>();
        for (Map.Entry<LogicalSequenceNumber, LogData> entry : logResult.entrySet()) {
            ILogData value = entry.getValue();
            if (value == null || value.isEmpty()) {
                value = read(runtimeLayout, entry.getKey());
            }

            returnResult.put(entry.getKey(), value);
        }

        return returnResult;
    }

    /**
     * Propagate a write down the chain, ignoring
     * any overwrite errors. It is expected that the
     * write has already successfully completed at
     * the head of the chain.
     *
     * @param runtimeLayout The epoch stamped client containing the layout to use for propagation.
     * @param globalAddress The global address to start
     *                      writing at.
     * @param data          The data to propagate, or NULL,
     *                      if it is to be a hole.
     */
    protected void propagate(RuntimeLayout runtimeLayout,
                             LogicalSequenceNumber globalAddress,
                             @Nullable ILogData data) {
        int numUnits = runtimeLayout.getLayout().getSegmentLength(globalAddress);

        for (int i = 1; i < numUnits; i++) {
            log.trace("Propogate[{}]: chain {}/{}", globalAddress, i + 1, numUnits);
            // In chain replication, we write synchronously to every unit
            // in the chain.
            try {
                if (data != null) {
                    CFUtils.getUninterruptibly(
                            runtimeLayout.getLogUnitClient(globalAddress, i)
                                    .write(data),
                            OverwriteException.class);
                } else {
                    CFUtils.getUninterruptibly(runtimeLayout
                            .getLogUnitClient(globalAddress, i)
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
     * @param runtimeLayout     The RuntimeLayout to use for the recovery.
     * @param globalAddress     The global address to drive
     *                          the recovery protocol
     *
     */
    protected void recover(RuntimeLayout runtimeLayout, LogicalSequenceNumber globalAddress) {
        final Layout layout = runtimeLayout.getLayout();
        // In chain replication, we started writing from the head,
        // and propagated down to the tail. To recover, we start
        // reading from the head, which should have the data
        // we are trying to recover
        int numUnits = layout.getSegmentLength(globalAddress);
        log.debug("Recover[{}]: read chain head {}/{}", globalAddress, 1, numUnits);
        ILogData ld = CFUtils.getUninterruptibly(runtimeLayout
                .getLogUnitClient(globalAddress, 0)
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
                        runtimeLayout.getLogUnitClient(globalAddress, i).write(ld),
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
    protected void holeFill(RuntimeLayout runtimeLayout, LogicalSequenceNumber globalAddress) {
        int numUnits = runtimeLayout.getLayout().getSegmentLength(globalAddress);
        log.trace("fillHole[{}]: chain head {}/{}", globalAddress, 1, numUnits);
        // In chain replication, we write synchronously to every unit in
        // the chain.
        try {
            CFUtils.getUninterruptibly(runtimeLayout
                    .getLogUnitClient(globalAddress, 0)
                    .fillHole(globalAddress), OverwriteException.class);
            propagate(runtimeLayout, globalAddress, null);
        } catch (OverwriteException oe) {
            // The hole-fill failed. We must ensure the other writer's
            // value is adopted before returning.
            recover(runtimeLayout, globalAddress);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validate(RuntimeLayout runtimeLayout, LogicalSequenceNumber address) throws InterruptedException, ExecutionException {
        // In chain replication we validate the existence of an address by querying 'ALL' log
        // units at the same time.
        //
        // Validation does not aim to assist in data propagation or hole filling, this is why
        // we don't only read from the tail. The idea is to verify that the address is present
        // in the current log.
        boolean addressValid;
        int numUnits = runtimeLayout.getLayout().getSegmentLength(address);
        List<CompletableFuture<TouchResponse>> touchCFAllLogUnits = new ArrayList<>();

        // Issue Touch Request to 'ALL' log units.
        // TODO: consider if requesting only to the head of the chain is enough, this would significanly
        // simplify this logic.
        for (int i = 0; i < numUnits; i++) {
            CompletableFuture<TouchResponse> cf = runtimeLayout
                    .getLogUnitClient(address, i)
                    .touch(address);
            touchCFAllLogUnits.add(cf);
        }

        // We don't need to wait on all log units to respond, the first log unit to validate
        // the existence of this address is sufficient. However, if the first to reply does not
        // validate its existence, this is not enough. We'll have to wait for other log units as
        // it might not have been propagated yet through all the chain.

        // We wait on the first future to complete and validate the existence of the address,
        // if no address exists we check for the other log units.
        CompletableFuture<Object> completeAny = CompletableFuture.anyOf(touchCFAllLogUnits.toArray(new CompletableFuture[numUnits]));
        TouchResponse tr = (TouchResponse) completeAny.get();

        // If address is validated on first log unit, return with validation successful.
        if (tr.getAddressesTouchMap().get(address)) {
            addressValid = true;
        } else {
            // Verify if touch request completed on all other log units.
            AbstractMap.SimpleEntry<Boolean, Integer> result = verifyTouchCompletedOnAllLogUnits(touchCFAllLogUnits, address);

            if (result.getKey()) {
                addressValid = true;
            } else if (result.getValue() == numUnits) {
                addressValid = false;
            } else {
                // If all log units have not responded yet, wait on all to complete
                CompletableFuture<Void> completeAll = CompletableFuture.allOf(touchCFAllLogUnits.toArray(new CompletableFuture[numUnits]));
                completeAll.get();

                addressValid = verifyTouchCompletedOnAllLogUnits(touchCFAllLogUnits, address).getKey() ? true : false;
            }
        }

        return addressValid;
    }

    /**
     * Verify the response of all log units to the touch request.
     *
     * @param touchCFLogUnits  List of completable futures for all log units.
     * @param address          Address to read.
     * @return                 A tuple representing the result of the address existence validation
     *                         (boolean) and the number of log units that responded.
     */
    private AbstractMap.SimpleEntry<Boolean, Integer> verifyTouchCompletedOnAllLogUnits(List<CompletableFuture<TouchResponse>> touchCFLogUnits,
                                                                                        LogicalSequenceNumber address) throws InterruptedException, ExecutionException{
        boolean validate = false;
        int completedFutureAddressNonExist = 0;
        for (CompletableFuture<TouchResponse> cfTR : touchCFLogUnits) {
            if (cfTR.isDone()) {
                if (cfTR.get().getAddressesTouchMap().get(address)) {
                    validate = true;
                    break;
                } else {
                    completedFutureAddressNonExist++;
                }
            }
        }

        return new AbstractMap.SimpleEntry<>(validate, completedFutureAddressNonExist);
    }
}

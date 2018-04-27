package org.corfudb.runtime.view.stream;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.StreamOptions;

/**
 * This class defines the space of addresses of a stream under the assumption that a stream can
 * be checkpoint(ed). It is made up of two separate spaces: that of regular addresses
 * and that of the checkpoint addresses. The CompositeStreamAddressSpace handles both spaces
 * to transparently provide a single resolved view of the stream address space to upper layers.
 *
 * Created by amartinezman on 4/24/18.
 */
public class CompositeStreamAddressSpace implements IStreamAddressSpace {

    private BackpointerStreamAddressSpace regularSAS;
    private CheckpointStreamAddressSpace cpSAS;
    private boolean pointerOnRegularStream;
    final private CorfuRuntime r;
    final private UUID regularStreamId;
    final private UUID cpStreamId;

    public CompositeStreamAddressSpace(UUID id, CorfuRuntime runtime) {
        regularStreamId = id;
        cpStreamId = CorfuRuntime.getCheckpointStreamIdFromId(id);
        r = runtime;
        regularSAS = new BackpointerStreamAddressSpace(regularStreamId, runtime);
        cpSAS = new CheckpointStreamAddressSpace(cpStreamId, runtime);
        pointerOnRegularStream = true;
    }

    @Override
    public void setStreamOptions(StreamOptions options) {
        this.regularSAS.setStreamOptions(options);
        this.cpSAS.setStreamOptions(options);
    }

    @Override
    public void reset() {
        regularSAS.reset();
        cpSAS.reset();
        pointerOnRegularStream = true;
    }

    @Override
    public void seek(long address) {
        // Seek for the address on the regular stream, if it is not available it means
        // it has been checkpoint(ed) and trimmed.
        try {
            regularSAS.seek(address);
            if (!pointerOnRegularStream) {
                // If pointer was following the CP SAS we should reset the pointer
                cpSAS.reset();
                pointerOnRegularStream = true;
            }
        } catch (RuntimeException re) {
            // If address is not valid on the regular stream, try to seek this address in the CP stream
            cpSAS.seek(address);
            // If this succeeds, move pointer in regular SAS to follow at the same point of the CP
            // and reset pointer to CP stream
            regularSAS.setPointerToPosition(cpSAS.getLastAddressSynced());
            pointerOnRegularStream = false;
        }
    }

    @Override
    public long getMax() {return regularSAS.getMax();}


    @Override
    public long getMin() {
        return regularSAS.getMin();
    }

    @Override
    public long getLastAddressSynced() {
        // If regularSAS has no address, maybe it was checkpoint(ed)/trimmed, the CP Stream Address
        // Space will have info regarding the last address that was synced for this stream and cp..
        if (pointerOnRegularStream) {
            return regularSAS.getLastAddressSynced();
        } else {
             return cpSAS.getLastAddressSynced();
        }
    }

    @Override
    public long next() {
        if (pointerOnRegularStream) {
            return regularSAS.next();
        } else {
            // If pointer is on the checkpoint stream and there is no next in this stream
            // we need to move onto the continuation  of the stream on the regularSAS
            if (cpSAS.hasNext()) {
                return cpSAS.next();
            } else if (regularSAS.hasNext()) {
                pointerOnRegularStream = true;
                return regularSAS.next();
            }
            return cpSAS.next();
        }
    }

    @Override
    public long previous() {
        if (pointerOnRegularStream) {
            if (regularSAS.hasPrevious()) {
                return regularSAS.previous();
            } else {
                // If there is no previous on regular stream check if you can move into CPStream
                if (cpSAS.hasPrevious()) {
                    pointerOnRegularStream = false;
                    return cpSAS.previous();
                } else {
                    return regularSAS.previous();
                }
            }
        } else {
            return cpSAS.previous();
        }
    }

    @Override
    public List<Long> remainingUpTo(long limit) {
        if(pointerOnRegularStream) {
            return regularSAS.remainingUpTo(limit);
        } else {
            // If pointer is on CPSAS we might need to move to regularSAS if we are
            // at the end of the CPSAS
            if (!cpSAS.hasNext()) {
                pointerOnRegularStream = true;
                return regularSAS.remainingUpTo(limit);
            }
            return cpSAS.remainingUpTo(limit);
        }
    }

    @Override
    public void addAddresses(List<Long> addresses) {
        regularSAS.addAddresses(addresses);
    }

    @Override
    public long getCurrentPointer() {
        if (pointerOnRegularStream) {
            return regularSAS.getCurrentPointer();
        } else {
            return cpSAS.getCurrentPointer();
        }
    }

    @Override
    public boolean hasNext() {
        if (pointerOnRegularStream) {
            return regularSAS.hasNext();
        } else if (cpSAS.hasNext()) {
            return cpSAS.hasNext();
        } else {
            // If we end here, we were traversing the CP stream but are now left without
            // next addresses on that stream, revert to the regular stream.
            pointerOnRegularStream = true;
            return regularSAS.hasNext();
        }
    }

    @Override
    public boolean hasPrevious() {
        if (!pointerOnRegularStream) {
            return cpSAS.hasPrevious();
        } else if (regularSAS.hasPrevious()) {
            return regularSAS.hasPrevious();
        } else {
            // Regular stream has no previous, move to CPStream
            pointerOnRegularStream = false;
            return cpSAS.hasPrevious();
        }
    }

    @Override
    public void removeAddresses(long upperBound) {
        this.regularSAS.removeAddresses(upperBound);
        this.cpSAS.removeAddresses(upperBound);
        if (regularSAS.isEmpty() && !cpSAS.isEmpty()) {
            pointerOnRegularStream = false;
        } else {
            pointerOnRegularStream = true;
        }
    }

    /**
     * <p>The composite stream address space is based on the notion that a stream's address space
     *    is two-fold, composed by the address space of: the regular stream and the checkpoint
     *    stream.</p>
     *
     *  <p> Therefore, to sync the stream implies:
     *         1. Sync the checkpoint SAS
     *         2. Based on the upper limit of checkpoint(ed) addresses, sync the regular SAS. This
     *            design decision is motivated by the following:
     *
     *             Not reading the complete space of addresses from the regular stream, (already
     *             contained in the CP stream) can give a performance advantage over synchronizing
     *             the whole space.
     *
     *             However, we should consider that if a snapshot transaction is requested on any
     *             of these addresses, we should be able to resolve them as long as they have not been
     *             trimmed.</p>
     *
     * @param globalAddress global address requested to access.
     * @param newTail stream tail.
     * @param lowerBound lower bound to sync up to. The range of addresses to sync is given by the space
     *                   between newTail and lowerBound.
     * @param readFn read function.
     */
    public void syncUpTo(long globalAddress, long newTail, long lowerBound,
                         Function<Long, ILogData> readFn) {

        // If the requested tail is lower than the last address synced (snapshot transaction),
        // sync the regular stream to get granular address resolution.
        if (newTail != Address.NOT_FOUND && newTail <= getLastAddressSynced()) {
            regularSAS.syncUpTo(globalAddress, newTail, lowerBound, readFn);
        } else if (!(globalAddress < cpSAS.getMax() && cpSAS.containsAddress(globalAddress))) {
            // Do not sync if globalAddress is contained in the space of the CP SAS
            long upperLimitAddressesCheckpointed = Address.NON_ADDRESS;

            // Get Regular Stream Tail
            if (newTail == Address.NOT_FOUND) {
                Token tokenRegular = r.getSequencerView()
                        .nextToken(Collections.singleton(regularStreamId), 0).getToken();
                newTail = tokenRegular.getTokenValue();
            }

            // Get CP Stream Tail
            // TODO: This is incurring in an extra sequencer call, but with PR #1277 this can be replaced
            // TODO: to request both stream tails under the same call.
            Token token = r.getSequencerView()
                    .nextToken(Collections.singleton(cpStreamId), 0).getToken();
            long cpTail = token.getTokenValue();

            // Sync the CP SAS, if tail exists for the CP stream
            if (cpTail != Address.NON_EXIST) {
                cpSAS.syncUpTo(globalAddress, cpTail, lowerBound, readFn);

                // Get the maximum address that has been checkpoint(ed) for the regular stream.
                // This will give the lower bound, so we sync the regular stream down until this limit
                upperLimitAddressesCheckpointed = cpSAS.getLastAddressSynced();

                if (upperLimitAddressesCheckpointed != Address.NON_ADDRESS) {
                    // If we are pointing to the regular stream and the pointer is set on an address
                    // lower or equal than upper address in the cp stream, move the pointer
                    // to the Checkpoint SAS, so we read data from the cp and set the pointer
                    // on regular stream to the limit covered by the CP
                    // TODO: we might need to rollback these positions as they have already been applied
                    // TODO: to the stream (look into this) or we could remove the cp addresses if regular stream
                    // TODO: is resolved for the range of the CP.
                    if (pointerOnRegularStream && regularSAS.getCurrentPointer() <= upperLimitAddressesCheckpointed) {
                        pointerOnRegularStream = false;
                        regularSAS.setPointerToPosition(upperLimitAddressesCheckpointed);
                    }

                    // Change lower bound if global address is not in the range of checkpoint addresses
                    // i.e., optimize the space of addresses being synced.
                    if (lowerBound < upperLimitAddressesCheckpointed && globalAddress > upperLimitAddressesCheckpointed) {
                        lowerBound = upperLimitAddressesCheckpointed;
                        pointerOnRegularStream = false;
                    }
                }
            }

            // Now, sync regular stream if requested tail is greater than the last synced address
            // for this stream
            if (regularSAS.getLastAddressSynced() < newTail) {
                try {
                    regularSAS.syncUpTo(globalAddress, newTail, lowerBound, readFn);
                } catch (TrimmedException te) {
                    // Throw exception if we were attempting to fulfill a snapshot transaction,
                    // in the space of checkpoint addresses
                    if (cpSAS.getLastAddressSynced() > globalAddress) {
                        throw te;
                    }
                }

                // If space of regular addresses is empty and CP stream contains addresses, point to CP.
                if (regularSAS.isEmpty() && !cpSAS.isEmpty()) {
                    pointerOnRegularStream = false;
                }

                // If we were able to build the regular log from the beginning with no trimmed
                // exceptions, then remove the checkpoint entries, this way we will be able
                // to provide snapshot transactions at the granularity of checkpoint(ed) entries
                if (globalAddress < upperLimitAddressesCheckpointed &&
                        regularSAS.getMin() > globalAddress) {
                    pointerOnRegularStream = true;
                    cpSAS.removeAddresses(cpSAS.getMax());
                } else {
                    regularSAS.setPointerToPosition(upperLimitAddressesCheckpointed);
                }
            }
        }
    }

    @Override
    public boolean containsAddress(long globalAddress) {
        return regularSAS.containsAddress(globalAddress);
    }

    @Override
    public long higher(long globalAddress, boolean forward) {
        return regularSAS.higher(globalAddress, forward);
    }

    @Override
    public long lower(long globalAddress, boolean forward) {
        return regularSAS.lower(globalAddress, forward);
    }

    @Override
    public int findAddresses(long oldTail, long newTail, Function<Long, ILogData> readFn) {
        throw new UnsupportedOperationException("findAddresses");
    }

    @Override
    public boolean isEmpty() {
        return this.regularSAS.isEmpty() && this.cpSAS.isEmpty();
    }

    @Override
    public void setPointerToPosition(long address) {
        this.regularSAS.setPointerToPosition(address);
    }
}

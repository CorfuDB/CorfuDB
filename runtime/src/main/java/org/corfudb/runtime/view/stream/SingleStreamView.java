package org.corfudb.runtime.view.stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AppendException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.StaleTokenException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.StreamOptions;


/**
 * This class represents a view of a single stream.
 *
 * <p>A SingleStreamView is tied to a stream address space, which contains an ordered set of
 *  all the addresses (global) with log entries for this stream. A stream address space is the
 *  entity which determines how streams are traversed. So, if a stream relies on backpointers
 *  to traverse its address space, the SingleStreamView will use the
 *  BackpointerStreamAddressSpace.</p>
 *
 *  <p>Current implementation of a stream view in Corfu, is defined with a two-fold space of
 *  addresses, one that tracks the addresses of the regular stream and another that tracks the
 *  addresses that have been checkpoint(ed). This composed space of addresses is represented by the
 *  CompositeStreamAddressSpace.</p>
 *
 * <p>Created by amartinezman on 4/24/18.</p>
 */
@Slf4j
public class SingleStreamView implements IStreamView {

    final private UUID id;
    final private CorfuRuntime runtime;
    private IStreamAddressSpace sas;
    final StreamOptions options;

    public SingleStreamView(UUID id, CorfuRuntime runtime,
                            BiFunction<UUID, CorfuRuntime, IStreamAddressSpace> streamAddressSpace,
                            @Nonnull final StreamOptions options) {
        this.id = id;
        this.runtime = runtime;
        this.sas = streamAddressSpace.apply(id, runtime);
        this.sas.setStreamOptions(options);
        this.options = options;
    }

    public SingleStreamView(UUID id, CorfuRuntime runtime, BiFunction<UUID, CorfuRuntime, IStreamAddressSpace> streamAddressSpace) {
        this(id, runtime, streamAddressSpace, StreamOptions.DEFAULT);
    }

    @Override
    public UUID getId() {
        return this.id;
    }

    @Override
    public void reset() {
        this.sas.reset();
    }

    @Override
    public long find(long globalAddress, IStreamView.SearchDirection direction) {
        // If address space is not solved until global address, sync...
        if (this.sas.getMax() < globalAddress) {
            long globalAddressToSync = globalAddress;
            if (direction.isForward()) {
                globalAddressToSync += 1;
            }
            remainingUpTo(globalAddressToSync);
        }

        // Now we can do the search.
        // First, check for inclusive searches.
        if (direction.isInclusive()
                && this.sas.containsAddress(globalAddress)) {
            return globalAddress;
        }

        // Next, check all elements excluding
        // in the correct direction.
        Long result;
        if (direction.isForward()) {
            result = this.sas.higher(globalAddress, true);
        }  else {
            result = this.sas.lower(globalAddress, false);
        }

        // Convert the address to never read if there was no result.
        return result == null ? Address.NOT_FOUND : result;
    }

    @Override
    public long append(Object object,
                       Function<TokenResponse, Boolean> acquisitionCallback,
                       Function<TokenResponse, Boolean> deacquisitionCallback) {
        // First, we get a token from the sequencer.
        TokenResponse tokenResponse = runtime.getSequencerView()
                .nextToken(Collections.singleton(id), 1);

        // We loop forever until we are interrupted, since we may have to
        // acquire an address several times until we are successful.
        for (int x = 0; x < runtime.getParameters().getWriteRetry(); x++) {
            // Next, we call the acquisitionCallback, if present, informing
            // the client of the token that we acquired.
            if (acquisitionCallback != null) {
                if (!acquisitionCallback.apply(tokenResponse)) {
                    // The client did not like our token, so we end here.
                    // We'll leave the hole to be filled by the client or
                    // someone else.
                    log.debug("Acquisition rejected token={}", tokenResponse);
                    return Address.NON_ADDRESS;
                }
            }

            // Now, we do the actual write. We could get an overwrite
            // exception here - any other exception we should pass up
            // to the client.
            try {
                runtime.getAddressSpaceView()
                        .write(tokenResponse, object);
                // The write completed successfully, so we return this
                // address to the client.
                this.sas.addAddresses(Arrays.asList(tokenResponse.getToken().getTokenValue()));
                return tokenResponse.getToken().getTokenValue();
            } catch (OverwriteException oe) {
                log.trace("Overwrite occurred at {}", tokenResponse);
                // We got overwritten, so we call the deacquisition callback
                // to inform the client we didn't get the address.
                if (deacquisitionCallback != null) {
                    if (!deacquisitionCallback.apply(tokenResponse)) {
                        log.debug("Deacquisition requested abort");
                        return Address.NON_ADDRESS;
                    }
                }
                // Request a new token, informing the sequencer we were
                // overwritten.
                tokenResponse = runtime.getSequencerView()
                        .nextToken(Collections.singleton(id),
                                1);
            } catch (StaleTokenException te) {
                log.trace("Token grew stale occurred at {}", tokenResponse);
                if (deacquisitionCallback != null && !deacquisitionCallback.apply(tokenResponse)) {
                    log.debug("Deacquisition requested abort");
                    return Address.NON_ADDRESS;
                }
                // Request a new token, informing the sequencer we were
                // overwritten.
                tokenResponse = runtime.getSequencerView()
                        .nextToken(Collections.singleton(id),
                                1);

            }
        }

        log.error("append[{}]: failed after {} retries, write size {} bytes",
                tokenResponse.getTokenValue(),
                runtime.getParameters().getWriteRetry(),
                ILogData.getSerializedSize(object));
        throw new AppendException();
    }

    @Override
    public ILogData previous() {
        long address = sas.previous();
        if (address == Address.NON_ADDRESS) {
            return null;
        } else {
            return read(address);
        }
    }

    @Override
    public ILogData current() {
        if (sas.getCurrentPointer() == Address.NON_ADDRESS) {
            return null;
        } else {
            return read(sas.getCurrentPointer());
        }
    }

    void syncAddressSpace(long globalAddress, long lowerBound) {
        // Sync Stream Address space from Global Address to specified lower bound (range),
        // if lower bound -1, sync to the beginning of the stream.
        if (globalAddress <= this.sas.getLastAddressSynced()) {
            return;
        } else {
            // We leverage the stream address space to decide up to which point to synchronize
            // the stream. The reasoning behind this is that a composite address space might request
            // tails for several streams at the same time (for instance, regular and checkpoint stream).
            // Making a single sequencer call instead of two is an important performance optimization.
            this.sas.syncUpTo(globalAddress, Address.NOT_FOUND, lowerBound, this::read);
        }
    }

    @Override
    public ILogData next() {
        if (sas.hasNext()) {
            return read(sas.next());
        } else {
            // Force a sequencer call
            syncAddressSpace(this.sas.getLastAddressSynced() + 1, Address.NON_ADDRESS);

            if (sas.hasNext()) {
                return read(sas.next());
            } else {
                return null;
            }
        }
    }

    @Override
    public void seek(long globalAddress) {
        syncAddressSpace(globalAddress, Address.NON_ADDRESS);
        sas.seek(globalAddress);
    }

    @Override
    public ILogData nextUpTo(long maxGlobal) {
        if (sas.hasNext()) {
            long address = sas.next();
            if (address > maxGlobal && (sas.getCurrentPointer() > maxGlobal)) {
                sas.previous();
                return null;
            }
            return read(address);
        } else {
            // Synchronize address space up to maxGlobal
            syncAddressSpace(maxGlobal, Address.NON_ADDRESS);
            if (sas.hasNext()) {
                long address = sas.next();
                if (address > maxGlobal && (sas.getCurrentPointer() > maxGlobal)) {
                    sas.previous();
                    return null;
                }
                return read(address);
            } else {
                return null;
            }
        }
    }


    @Override
    public List<ILogData> remainingUpTo(long maxGlobal) {
        syncAddressSpace(maxGlobal, Address.NON_ADDRESS);
        List<Long> addressesToRead = sas.remainingUpTo(maxGlobal);
        List<ILogData> res = new ArrayList<>(addressesToRead.size());
        for (Long address : addressesToRead) {
            res.add(read(address));
        }
        return res;
    }

    @Override
    public List<ILogData> remaining() {
        throw new UnsupportedOperationException("remaining()");
    }

    @Override
    public boolean hasNext() {
        return sas.hasNext();
    }

    @Override
    public long getCurrentGlobalPosition() {
        return sas.getCurrentPointer();
    }

    protected ILogData read(final long address) {
        try {
            return runtime.getAddressSpaceView().read(address);
        } catch (TrimmedException te) {
            // If a trimmed exception is encountered remove address from the space of valid addresses
            // as it is no longer available.
            this.sas.removeAddresses(address);
            throw te;
        }
    }
}

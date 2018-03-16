package org.corfudb.runtime.view.stream;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AppendException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.StaleTokenException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

@Slf4j
public class SimpleView implements IStreamView {

    final private UUID id;

    final private CorfuRuntime runtime;

    final private StreamAddressSpace sas;

    long globalMax = -1;

    public SimpleView(UUID id, CorfuRuntime runtime) {
        this.id = id;
        this.runtime = runtime;
        this.sas = new StreamAddressSpace();
    }


    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public void reset() {
        sas.reset();
    }


    @Override
    public long find(long globalAddress, IStreamView.SearchDirection direction) {
        throw new UnsupportedOperationException("find");
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
                    return -1L;
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
                return tokenResponse.getToken().getTokenValue();
            } catch (OverwriteException oe) {
                log.trace("Overwrite occurred at {}", tokenResponse);
                // We got overwritten, so we call the deacquisition callback
                // to inform the client we didn't get the address.
                if (deacquisitionCallback != null) {
                    if (!deacquisitionCallback.apply(tokenResponse)) {
                        log.debug("Deacquisition requested abort");
                        return -1L;
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
                    return -1L;
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
        if (sas.getCP() == Address.NON_ADDRESS) {
            return null;
        } else {
            return read(sas.getCP());
        }
    }

    void syncAddressSpace(long globalAddress) {
        if (globalAddress <= globalMax) {
            return;
        } else {
            long oldTail = sas.getMax();
            Token token = runtime.getSequencerView()
                    .nextToken(Collections.singleton(id), 0).getToken();
            long newTail = token.getTokenValue();

            List<Long> addressesToAdd = new ArrayList<>(100);

            while (newTail > oldTail) {
                ILogData d = read(newTail);
                if (d.containsStream(id)) {
                    addressesToAdd.add(newTail);
                }
                // What happens when a hole is encountered ?
                newTail = d.getBackpointer(id);
            }

            // update global max and add the new tail addresses
            // need to consider the global tail too !!!!!!!
            //globalMax = Math.max(globalMax, token.getGlobalTail());
            //System.out.println("---> globalAddress " + globalAddress);
            globalMax = Math.max(globalMax, globalAddress);
            List<Long> revList = Lists.reverse(addressesToAdd);
            sas.addAddresses(revList);
        }
    }

    @Override
    public ILogData next() {
        if (sas.hasNext()) {
            return read(sas.next());
        } else {
            // Force a sequencer call
            syncAddressSpace(globalMax + 1);

            if (sas.hasNext()) {
                return read(sas.next());
            } else {
                return null;
            }
        }
    }

    @Override
    public void seek(long globalAddress) {
        syncAddressSpace(globalMax + 1);
        sas.seek(globalAddress);
    }

    @Override
    public ILogData nextUpTo(long maxGlobal) {
        if (sas.hasNext()) {
            long address = sas.next();
            if (address > maxGlobal) {
                sas.previous();
                return null;
            }
            return read(address);
        } else {
            syncAddressSpace(maxGlobal);
            if (sas.hasNext()) {
                long address = sas.next();
                if (address > maxGlobal) {
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
        syncAddressSpace(maxGlobal);
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
        return sas.getCP();
    }


    protected ILogData read(final long address) {
        try {
            return runtime.getAddressSpaceView().read(address);
        } catch (TrimmedException te) {
            throw te;
        }
    }


    class StreamAddressSpace {
        private ArrayList<Long> addresses;
        private int maxInd;
        private int currInd;

        public StreamAddressSpace() {
            addresses = new ArrayList<>(10_000);
            maxInd = -1;
            currInd = -1;
        }

        public void reset() {
            currInd = -1;
        }

        void seek(long address) {
            int tmpPtr = maxInd;

            while(tmpPtr >= 0) {
                if(addresses.get(tmpPtr) == address) {
                    currInd = tmpPtr;
                    return;
                }
                tmpPtr = tmpPtr - 1;
            }

            throw new RuntimeException("Couldn't seek " + id + " to address " + address);
        }

        void addAddresses(List<Long> addresses) {
            if (!addresses.isEmpty()) {
                this.addresses.addAll(addresses);
                maxInd += addresses.size();
            }
        }

        public long getMax() {
            if (maxInd == -1) {
                return Address.NON_ADDRESS;
            } else {
                return addresses.get(maxInd);
            }
        }

        /**
         * Returns the address that the current pointer is pointing to.
         *
         * @return A stream address
         */
        public long getCP() {
            if (currInd == -1) {
                return Address.NON_ADDRESS;
            } else {
                return addresses.get(currInd);
            }
        }

        /**
         * returns the next address in the stream
         *
         * @return
         */
        public long next() {
            // NOTE: unlike previous current will be maxed to maxInd, which can
            // affect subsequent current calls
            if (currInd + 1 > maxInd) {
                return Address.NON_ADDRESS;
            } else {
                currInd++;
                return addresses.get(currInd);
            }
        }

        public long previous() {
            if (currInd - 1 >= 0) {
                currInd--;
                return addresses.get(currInd);
            } else {
                currInd = -1;
                return Address.NON_ADDRESS;
            }
        }

        public boolean hasNext() {
            if (currInd < maxInd) {
                return true;
            } else {
                return false;
            }
        }

        public List<Long> remainingUpTo(long limit) {
            List<Long> res = new ArrayList<>(100);
            while (hasNext() && addresses.get(currInd + 1) <= limit) {
                res.add(addresses.get(currInd + 1));
                currInd++;
            }
            return res;
        }
    }

}

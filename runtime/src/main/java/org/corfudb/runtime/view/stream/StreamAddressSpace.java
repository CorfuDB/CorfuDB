package org.corfudb.runtime.view.stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Synchronized;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.StreamOptions;

/**
 * This is an abstract class which represents the space of addresses of a stream. It is based on the
 * assumption that the address space of a stream can be traversed forward and backwards.
 *
 * The abstract method 'findAddresses' defines how to traverse the space of unexplored addresses,
 * i.e, addresses between the latest synced point (oldTail) and the latest update (newTail).
 * For example, the implementation of BackpointerAddressSpace assumes the use of backpointers
 * to determine the previous log entry for a stream.
 *
 * Created by amartinezman on 4/23/18.
 */
public abstract class StreamAddressSpace implements IStreamAddressSpace {

    protected List<Long> addresses;
    protected AtomicInteger maxInd;
    protected AtomicInteger currInd;
    // Limit up to which the address space has been resolved. Any address beyond this limit
    // is only a hint, i.e., we need to sync with the sequencer to be sure other clients have not
    // written to this stream in addresses in-between. Hints allow to take advantage of local writes.
    protected AtomicInteger resolvedAddressLimit;
    protected UUID streamId;
    protected CorfuRuntime runtime;
    protected StreamOptions options;

    public StreamAddressSpace(UUID id, CorfuRuntime runtime) {
        this.streamId = id;
        this.runtime = runtime;
        this.addresses = Collections.synchronizedList(new ArrayList<>());
        this.currInd = new AtomicInteger(-1);
        this.maxInd = new AtomicInteger(-1);
        this.resolvedAddressLimit = new AtomicInteger(-1);
    }

    @Override
    public void setStreamOptions(StreamOptions options) {
        this.options = options;
    }

    @Override
    public void reset() {
        currInd.set(-1);
    }

    @Override
    public void seek(long address) {

        int tmpPtr = addresses.indexOf(address);

        if (tmpPtr != -1) {
            if(resolvedAddressLimit.get() < tmpPtr) {
                // If address to seek does not fall in the space of resolved addresses, sync to tail
                Token token = runtime.getSequencerView()
                        .nextToken(Collections.singleton(streamId), 0).getToken();
                long currentTail = token.getTokenValue();
                long lowerBound = (resolvedAddressLimit.get() == -1) ? Address.NON_ADDRESS :
                        addresses.get(resolvedAddressLimit.get());
                syncUpTo(currentTail, currentTail, lowerBound);
            }
            currInd.set(tmpPtr);
            return;
        }

        throw new RuntimeException("Couldn't seek " + streamId + " to address " + address);
    }

    @Override
    public long getMax() {
        if (maxInd.get() == -1) {
            return Address.NON_ADDRESS;
        } else {
            return addresses.get(maxInd.get());
        }
    }

    @Override
    public long getMin() {
        if (addresses.size() == 0) {
            return Address.NON_ADDRESS;
        } else {
            return addresses.get(0);
        }
    }

    @Override
    public long getLastAddressSynced() {
        return getMax();
    }

    @Override
    public long next() {
        if (currInd.get() + 1 > maxInd.get()) {
            return Address.NON_ADDRESS;
        } else {
            if(resolvedAddressLimit.get() < currInd.get() + 1) {
                // If 'next' address does not fall in the space of resolved addresses (hint),
                // sync the address space from resolvedAddress to tail
                Token token = runtime.getSequencerView()
                        .nextToken(Collections.singleton(streamId), 0).getToken();
                long currentTail = token.getTokenValue();
                long lowerBound = (resolvedAddressLimit.get() == -1) ? Address.NON_ADDRESS : addresses.get(resolvedAddressLimit.get());
                syncUpTo(currentTail, currentTail, lowerBound);
            }
            return addresses.get(currInd.incrementAndGet());
        }
    }

    @Override
    @Synchronized
    public long previous() {
        if (currInd.get() - 1 >= 0) {
            return addresses.get(currInd.decrementAndGet());
        } else {
            currInd.set(-1);
            return Address.NON_ADDRESS;
        }
    }

    @Override
    public List<Long> remainingUpTo(long limit) {
        List<Long> res = new ArrayList<>(100);
        while (hasNext() && addresses.get(currInd.get() + 1) <= limit) {
            res.add(addresses.get(currInd.get() + 1));
            currInd.incrementAndGet();
        }
        return res;
    }

    @Override
    @Synchronized
    public void addAddresses(List<Long> addresses) {
        this.addresses.addAll(addresses);
        maxInd.set(this.addresses.size() - 1);
    }

    @Override
    public long getCurrentPointer() {
        if (currInd.get() == -1) {
            return Address.NON_ADDRESS;
        } else {
            return addresses.get(currInd.get());
        }
    }

    @Override
    public boolean hasNext() {
        if (currInd.get() < maxInd.get()) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean hasPrevious() {
        return currInd.get() > 0;
    }

    @Override
    @Synchronized
    public void removeAddresses(long upperBound) {
        int removedAddresses = 0;
        int index = this.addresses.indexOf(upperBound);
        if (index != -1) {
            for (int i = index; i >= 0; i--){
                removedAddresses ++;
                this.addresses.remove(i);
            }

            // Reset indexes if current pointer is in the space of addresses to be removed
            // else, rebase index
            if (currInd.get() <= index) {
                // Reset current index to first entry in the stream
                currInd.set(-1);
            } else {
                currInd.getAndAdd(-removedAddresses);
            }

            if (resolvedAddressLimit.get() <= index) {
                resolvedAddressLimit.set(-1);
            } else {
                resolvedAddressLimit.getAndAdd(-removedAddresses);
            }

            maxInd.set(this.addresses.size() - 1);
        }
    }

    @Override
    @Synchronized
    public void removeAddress(long address) {
        int tmpIndex = this.addresses.indexOf(address);
        if (tmpIndex != -1) {
            this.addresses.remove(tmpIndex);

            if (currInd.get() >= tmpIndex) {
                currInd.decrementAndGet();
            }
            maxInd.set(this.addresses.size() - 1);
        }
    }

    @Override
    public void syncUpTo(long globalAddress, long newTail, long lowerBound) {
        // Ensure there is an upper limit to sync up to
        if (newTail != Address.NOT_FOUND) {
            // Case: tail ahead of max synced address for this space
            if (getMax() < newTail) {
                long oldTail = Address.NON_ADDRESS;
                // Set old tail to track the min between the specified lower bound and the latest
                // resolved address
                if (resolvedAddressLimit.get() != -1) {
                    oldTail = Math.min(addresses.get(resolvedAddressLimit.get()), lowerBound);
                }
                findAddresses(globalAddress, oldTail, newTail);

            } else if (getMax() != newTail || resolvedAddressLimit.get() != maxInd.get()) {
                // Case: snapshot
                int addressesAddedBeforePointer = findAddresses(globalAddress, lowerBound, newTail);
                if (currInd.get() != -1) {
                    currInd.getAndAdd(addressesAddedBeforePointer);
                }
            }
            // Sort address space (limit to the space of recently resolved addresses),
            // as all addresses before this limit have been ordered...
            synchronized (addresses) {
                if (this.addresses.size() != 0 && (resolvedAddressLimit.get() + 1 != maxInd.get())) {
                    int lowerLimit = resolvedAddressLimit.get() != -1 ? resolvedAddressLimit.get() + 1 : 0;
                    // + 1 because sublist toIndex is exclusive
                    Collections.sort(this.addresses.subList(lowerLimit, maxInd.get() + 1));
                }
                // update resolved address to current tail
                resolvedAddressLimit.set(addresses.indexOf(newTail));
            }
        }
    }

    @Override
    public boolean containsAddress(long globalAddress) {
        return addresses.contains(globalAddress);
    }

    @Override
    public long higher(long globalAddress, boolean forward) {
        int index = addresses.indexOf(globalAddress);
        if (index == -1) {
            index = getNearestAddress(globalAddress, forward);
        }
        if (index != -1) {
            if (addresses.size() > index + 1) {
                // Next index exists
                return addresses.get(index + 1);
            }
        }
        return Address.NOT_FOUND;
    }

    @Override
    public long lower(long globalAddress, boolean forward) {
        int index = addresses.indexOf(globalAddress);
        if (index == -1) {
            index = getNearestAddress(globalAddress, forward);
        }
        if (index != -1) {
            if (index > 0) {
                // Next index exists
                return addresses.get(index - 1);
            }
        }
        return Address.NOT_FOUND;
    }

    @Override
    public boolean isEmpty() {
        return this.addresses.isEmpty();
    }

    @Override
    public abstract int findAddresses(long globalAddress, long oldTail, long newTail);

    /**
     * Find the nearest address to 'globalAddress' in the complete space of addresses.
     * Returns the index of the address.
     */
    private int getNearestAddress(long globalAddress, boolean forward) {
        int lowPointer = 0;
        int highPointer = addresses.size() - 1;

        if (highPointer < 0)
            return -1;

        while (lowPointer < highPointer) {
            int midPointer = (lowPointer + highPointer) / 2;
            assert(midPointer < highPointer);
            long d1 = Math.abs(addresses.get(midPointer) - globalAddress);
            long d2 = Math.abs(addresses.get(midPointer+1) - globalAddress);
            if (d2 < d1)
            {
                lowPointer = midPointer+1;
            } else if (d2 == d1) {
                // Depending on the search direction determine the nearest to be the greatest or lowest.
                if (forward) {
                    highPointer = midPointer;
                } else {
                    lowPointer = midPointer+1;
                }
            } else {
                highPointer = midPointer;
            }
        }
        return highPointer;
    }

    @Override
    public void setPointerToPosition(long address) {
        currInd.set(addresses.indexOf(address));
    }

    @Override
    public ILogData read(final long address) {
        try {
            return runtime.getAddressSpaceView().read(address);
        } catch (TrimmedException te) {
            // If a trimmed exception is encountered remove address from the space of valid addresses
            // as it is no longer available.
            this.removeAddresses(address);
            throw te;
        }
    }
}
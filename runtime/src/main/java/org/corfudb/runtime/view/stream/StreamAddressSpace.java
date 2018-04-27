package org.corfudb.runtime.view.stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import lombok.Synchronized;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
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

    protected CopyOnWriteArrayList<Long> addresses;
    protected UUID streamId;
    protected CorfuRuntime runtime;
    protected int maxInd;
    protected int currInd;
    protected StreamOptions options;

    public StreamAddressSpace(UUID id, CorfuRuntime runtime) {
        this.streamId = id;
        this.runtime = runtime;
        this.addresses = new CopyOnWriteArrayList();
        this.currInd = -1;
        this.maxInd = -1;
    }

    @Override
    public void setStreamOptions(StreamOptions options) {
        this.options = options;
    }

    @Override
    public void reset() {
        currInd = -1;
    }

    @Override
    public void seek(long address) {

        int tmpPtr = addresses.indexOf(address);

        if (tmpPtr != -1) {
            currInd = tmpPtr;
            return;
        }

        throw new RuntimeException("Couldn't seek " + streamId + " to address " + address);
    }

    @Override
    public long getMax() {
        if (maxInd == -1) {
            return Address.NON_ADDRESS;
        } else {
            return addresses.get(maxInd);
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
    @Synchronized
    public long next() {
        if (currInd + 1 > maxInd) {
            return Address.NON_ADDRESS;
        } else {
            currInd++;
            return addresses.get(currInd);
        }
    }

    @Override
    @Synchronized
    public long previous() {
        if (currInd - 1 >= 0) {
            currInd--;
            return addresses.get(currInd);
        } else {
            currInd = -1;
            return Address.NON_ADDRESS;
        }    }

    @Override
    public List<Long> remainingUpTo(long limit) {
        List<Long> res = new ArrayList<>(100);
        while (hasNext() && addresses.get(currInd + 1) <= limit) {
            res.add(addresses.get(currInd + 1));
            currInd++;
        }
        return res;
    }

    @Override
    public void addAddresses(List<Long> addresses) {
        this.addresses.addAll(addresses);
        Collections.sort(this.addresses);
        maxInd += addresses.size();
    }

    @Override
    public long getCurrentPointer() {
        if (currInd == -1) {
            return Address.NON_ADDRESS;
        } else {
            return addresses.get(currInd);
        }
    }

    @Override
    public boolean hasNext() {
        if (currInd < maxInd) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean hasPrevious() {
        if (currInd > 0 && currInd < addresses.size()) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void removeAddresses(long upperBound) {
        int removedAddresses = 0;
        int index = this.addresses.indexOf(upperBound);
        if (index != -1) {
            for (int i = index; i >= 0; i--){
                removedAddresses ++;
                this.addresses.remove(i);
            }

            if (currInd <= index) {
                // Reset current index to first entry in the stream
                currInd = -1;
            } else {
                currInd = currInd - removedAddresses;
            }

            maxInd = this.addresses.size() - 1;
        }
    }

    @Override
    public void syncUpTo(long globalAddress, long newTail, long lowerBound,
                         Function<Long, ILogData> readFn) {
        // Ensure there is an upper limit to sync up to
        if (newTail != Address.NON_EXIST) {
            if (getMax() < newTail ) {
                long oldTail = lowerBound;

                if (maxInd != -1) {
                    if (lowerBound > addresses.get(maxInd)) {
                        oldTail = lowerBound;
                    } else {
                        oldTail = addresses.get(maxInd);
                    }
                }

                findAddresses(oldTail, newTail, readFn);

            } else if (getMax() != newTail) {
                int addressesAdded = findAddresses(lowerBound, newTail, readFn);

                if (currInd != -1) {
                    currInd = currInd + addressesAdded;
                }
                maxInd += this.addresses.size();
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
    public abstract int findAddresses(long oldTail, long newTail, Function<Long, ILogData> readFn);

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
        currInd = addresses.indexOf(address);
    }

}
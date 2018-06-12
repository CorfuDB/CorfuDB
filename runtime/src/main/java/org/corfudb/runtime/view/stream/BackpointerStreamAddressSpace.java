package org.corfudb.runtime.view.stream;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;

/**
 * The BackpointerStreamAddressSpace follows backpointers in order to
 * traverse a stream in a given range (defined by the space between oldTail and newTail).
 *
 * Created by amartinezman on 4/18/18.
 */
@Slf4j
public class BackpointerStreamAddressSpace extends StreamAddressSpace {

    public BackpointerStreamAddressSpace(UUID id, CorfuRuntime runtime) {
        super(id, runtime);
    }

    @Override
    public int findAddresses(long globalAddress, long oldTail, long newTail) {
        List<Long> addressesToAdd = new ArrayList<>(100);
        // Keep track of stream tail to update max resolved address at the end
        long maxResolvedAddress = newTail;

        // Bound address search to the previous tail of the stream
        while (newTail > oldTail) {
            try {
                ILogData d = read(newTail);
                if (d.getType() != DataType.HOLE) {
                   if (d.containsStream(streamId)) {
                       if (!this.addresses.contains(newTail)) {
                           addressesToAdd.add(newTail);
                       }
                   }
                   try {
                       newTail = d.getBackpointer(streamId);
                   } catch (NullPointerException ne) {
                       // No further backpointer found...
                       break;
                   }
                } else {
                    // When a hole is encountered we downgrade to single step
                    newTail = oldTail - 1;
                }
            } catch (TrimmedException te) {
                if (options.ignoreTrimmed) {
                    log.warn("followBackpointers: Ignoring trimmed exception for address[{}]," +
                            " stream[{}]", newTail, streamId);
                    this.removeAddresses(newTail);
                    break;
                } else {
                    // If the trimmed address (newTail) is lower than the requested globalAddress,
                    // and it does not belong to the regular stream, it means it is either part of CPStream
                    // or not currently present because a later CP super-seeds, in this case, ignore the trim
                    if (newTail < globalAddress) {
                        if (!(this.containsAddress(globalAddress) || addressesToAdd.contains(globalAddress))) {
                            break;
                        } else {
                            // Add the discovered addresses before trim point
                            List<Long> revList = Lists.reverse(addressesToAdd);
                            this.addAddresses(revList);
                            // Update resolved address
                            resolvedAddressLimit.set(addresses.indexOf(maxResolvedAddress));
                            throw te;
                        }
                    } else {
                        // The requested global address is in the range of trimmed addresses
                        throw te;
                    }
                }
            }
        }
        
        this.addAddresses(Lists.reverse(addressesToAdd));
        long currentPointerAddress = getCurrentPointer();
        // Count number of addresses synced/added before the current pointer (this is the case for snapshot transactions)
        // This count will be used to rebase the current pointer index
        return (int) addressesToAdd.stream().filter(a -> a < currentPointerAddress).count();
    }
}

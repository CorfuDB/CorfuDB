package org.corfudb.runtime.view.stream;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

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
    public int findAddresses(long oldTail, long newTail, Function<Long, ILogData> readFn) {
        List<Long> addressesToAdd = new ArrayList<>(100);

        while (newTail > oldTail) {
            try {
                ILogData d = readFn.apply(newTail);
                if (d.getType() != DataType.HOLE) {
                   if (d.containsStream(streamId)) {
                       addressesToAdd.add(newTail);
                   }
                   newTail = d.getBackpointer(streamId);
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
                    throw te;
                }
            }
        }
        
        List<Long> revList = Lists.reverse(addressesToAdd);
        this.addAddresses(revList);
        return revList.size();
    }

}

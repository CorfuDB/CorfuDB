package org.corfudb.runtime.view.stream;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.Utils;

@Slf4j
/**
 * The Checkpoint Stream Address Space resolves the space of addresses of a checkpoint(ed) stream,
 * therefore it is capable of following backpointers and resolving the checkpoint at the same time,
 * i.e., determine how to traverse the cp stream
 *
 * For the checkpoint stream address space we override the methods: next, removeAddresses and syncUpTo
 * because for the checkpoint stream there is no notion of resolvedAddressLimit, whatever is present is conclusive.
 *
 * Created by amartinezman on 4/18/18.
 */
public class CheckpointStreamAddressSpace extends StreamAddressSpace {

    /** Info on checkpoint we used for initial stream replay,
     *  other checkpoint-related info & stats.  Hodgepodge, clarify.
     */
    @Getter
    public long lastCPAddress;
    UUID checkpointSuccessId = null;
    long checkpointSuccessStartAddr = Address.NEVER_READ;
    long checkpointSuccessEndAddr = Address.NEVER_READ;
    long checkpointSuccessNumEntries = 0L;
    long checkpointSuccessBytes = 0L;

    /** The address the current checkpoint snapshot was taken at.
     *  The checkpoint guarantees for this stream there are no entries
     *  between checkpointSuccessStartAddr and checkpointSnapshotAddress.
     */
    long checkpointSnapshotAddress = Address.NEVER_READ;

    public CheckpointStreamAddressSpace(UUID id, CorfuRuntime runtime) {
        super(id, runtime);
        lastCPAddress = Address.NON_ADDRESS;
    }

    @Override
    /**
     * Returns the highest checkpoint(ed) address, i.e., the upper limit of the range of addresses
     * that are contained in this checkpoint (from the regular stream).
     */
    public long getLastAddressSynced() {
        if (maxInd.get() == -1) {
            return Address.NON_ADDRESS;
        } else {
            return lastCPAddress;
        }
    }

    private void setCheckpointBoundary(ILogData firstCPEntry) {
        if (firstCPEntry.hasCheckpointMetadata()) {
            CheckpointEntry cpEntry = (CheckpointEntry)
                    firstCPEntry.getPayload(runtime);

            if (cpEntry.getCpType() == CheckpointEntry.CheckpointEntryType.START) {
                lastCPAddress = Long.decode(cpEntry.getDict().get(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS));
                return;
            }
        }
    }

    @Override
    public long next() {
        if (currInd.get() + 1 > maxInd.get()) {
            return Address.NON_ADDRESS;
        } else {
            currInd.incrementAndGet();
            return addresses.get(currInd.get());
        }
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

            // Reset indexes if pointing to tha space of addresses being removed
            // else, rebase.
            if (currInd.get() <= index) {
                // Reset current index to first entry in the stream
                currInd.set(-1);
            } else {
                currInd.getAndAdd(-removedAddresses);
            }

            maxInd.set(this.addresses.size() - 1);
        }
    }

    @Override
    public void syncUpTo(long globalAddress, long newTail, long lowerBound) {
        // Ensure there is an upper limit to sync up to
        if (newTail != Address.NON_EXIST) {
            if (getMax() < newTail) {
                long oldTail = lowerBound;

                if (maxInd.get() != -1) {
                    if (lowerBound > addresses.get(maxInd.get())) {
                        oldTail = lowerBound;
                    } else {
                        oldTail = getMax();
                    }
                }

                findAddresses(globalAddress, oldTail, newTail);

            } else if (getMax() != newTail) {
                int addressesAdded = findAddresses(globalAddress, lowerBound, newTail);

                if (currInd.get() != -1) {
                    currInd.getAndAdd(addressesAdded);
                }
            }
        }
    }

    @Override
    public long getCurrentPointer() {
        // The current pointer refers to the current version of the object, which is in fact related
        // to the last global address synced for this checkpoint stream.
        return getLastAddressSynced();
    }

    @Override
    public int findAddresses(long globalAddress, long oldTail, long newTail) {
        List<Long> addressesToAdd = new ArrayList<>(100);

        while (newTail > oldTail) {
            try {
                ILogData d = read(newTail);
                if (d.getType() != DataType.HOLE) {
                    if (d.containsStream(streamId)) {
                        BackpointerOp op = resolveCheckpoint(d, newTail);
                        if (op == BackpointerOp.INCLUDE
                                || op == BackpointerOp.INCLUDE_STOP) {
                            addressesToAdd.add(newTail);
                            if(op == BackpointerOp.INCLUDE_STOP) {
                                break;
                            }
                        }
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
                } else {
                    this.removeAddresses(newTail);
                    List<Long> revList = Lists.reverse(addressesToAdd);
                    this.addAddresses(revList);
                    throw te;
                }
            }
        }

        List<Long> revList = Lists.reverse(addressesToAdd);
        this.addAddresses(revList);
        return revList.size();
    }

    protected BackpointerOp resolveCheckpoint(ILogData data, long maxGlobal) {
        if (data.hasCheckpointMetadata()) {
            CheckpointEntry cpEntry = (CheckpointEntry)
                    data.getPayload(runtime);

            // Select the latest cp that has a snapshot address
            // which is less than maxGlobal
            if (this.checkpointSuccessId == null &&
                    cpEntry.getCpType() == CheckpointEntry.CheckpointEntryType.END
                    && Long.decode(cpEntry.getDict().get(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS)) <= maxGlobal) {
                // First time a checkpoint is found for this stream
                log.trace("Checkpoint[{}] END found at address {} type {} id {} author {}",
                        this, data.getGlobalAddress(), cpEntry.getCpType(),
                        Utils.toReadableId(cpEntry.getCheckpointId()),
                        cpEntry.getCheckpointAuthorId());
                this.checkpointSuccessId = cpEntry.getCheckpointId();
                this.checkpointSuccessNumEntries = 1L;
                this.checkpointSuccessBytes = (long) data.getSizeEstimate();
                this.checkpointSuccessEndAddr = data.getGlobalAddress();
            }
            else if (data.getCheckpointId().equals(this.checkpointSuccessId)) {
                this.checkpointSuccessNumEntries++;
                this.checkpointSuccessBytes += cpEntry.getSmrEntriesBytes();
                if (cpEntry.getCpType().equals(CheckpointEntry.CheckpointEntryType.START)) {
                    this.checkpointSuccessStartAddr = Long.decode(cpEntry.getDict()
                            .get(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS));
                    if (cpEntry.getDict().get(CheckpointEntry.CheckpointDictKey
                            .SNAPSHOT_ADDRESS) != null) {
                        this.checkpointSnapshotAddress = Long.decode(cpEntry.getDict()
                                .get(CheckpointEntry.CheckpointDictKey.SNAPSHOT_ADDRESS));
                    }
                    log.trace("Checkpoint[{}] HALT due to START at address {} startAddr"
                                    + " {} type {} id {} author {}",
                            this, data.getGlobalAddress(), this.checkpointSuccessStartAddr,
                            cpEntry.getCpType(),
                            Utils.toReadableId(cpEntry.getCheckpointId()),
                            cpEntry.getCheckpointAuthorId());
                    setCheckpointBoundary(data);
                    // We have reached the start of the checkpoint, include address and
                    // stop following backpointers
                    return BackpointerOp.INCLUDE_STOP;
                }
            } else {
                return BackpointerOp.EXCLUDE;
            }
        }
        // Not a checkpoint entry
        return BackpointerOp.INCLUDE;
    }

    protected enum BackpointerOp {
        INCLUDE,    /** Include this address. */
        EXCLUDE,    /** Exclude this address. */
        INCLUDE_STOP    /** Stop, but also include this address. */
    }
}

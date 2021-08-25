package org.corfudb.runtime.view.replication;

import com.google.common.collect.Iterables;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.InspectAddressesResponse;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.RecoveryException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.util.CFUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created by mwei on 4/6/17.
 */
@Slf4j
public class ChainReplicationProtocol extends AbstractReplicationProtocol {

    public ChainReplicationProtocol(IHoleFillPolicy holeFillPolicy) {
        super(holeFillPolicy);
    }

    private final ConcurrentHashMap<String, Timer> perNodeWriteTimer =
            new ConcurrentHashMap<>();

    /**
     * {@inheritDoc}
     */

    private void registerTimerPerNode(RuntimeLayout runtimeLayout) {
        double[] percentiles = new double[]{0.50, 0.99};
        MeterRegistryProvider.getInstance().ifPresent(registry -> {
            for (String server : runtimeLayout.getLayout().getAllLogServers()) {
                perNodeWriteTimer.putIfAbsent(server,
                        Timer.builder("chain_replication.write")
                                .tag("node", server)
                                .publishPercentiles(percentiles)
                                .publishPercentileHistogram(true)
                                .register(registry));
            }
        });
    }

    private void doWrite(RuntimeLayout runtimeLayout, long address, int index, Runnable writeRunnable) {
        String server = runtimeLayout.getLayout().getStripe(address).getLogServers().get(index);
        if (perNodeWriteTimer.containsKey(server)) {
            perNodeWriteTimer.get(server).record(writeRunnable);
        } else {
            writeRunnable.run();
        }
    }

    @Override
    public void write(RuntimeLayout runtimeLayout, ILogData data) throws OverwriteException {
        final long globalAddress = data.getGlobalAddress();
        int numUnits = runtimeLayout.getLayout().getSegmentLength(globalAddress);

        registerTimerPerNode(runtimeLayout);
        // To reduce the overhead of serialization, we serialize only the
        // first time we write, saving when we go down the chain.
        try (ILogData.SerializationHandle sh = data.getSerializedForm(true)) {
            log.trace("Write[{}]: chain head {}/{}", globalAddress, 1, numUnits);
            // In chain replication, we start at the chain head.
            try {
                final int headIndex = 0;
                Runnable writable = () -> CFUtils.getUninterruptibly(
                        runtimeLayout.getLogUnitClient(globalAddress, headIndex)
                                .write(sh.getSerialized()),
                        OverwriteException.class);
                doWrite(runtimeLayout, globalAddress, headIndex, writable);
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
    public ILogData peek(RuntimeLayout runtimeLayout, long globalAddress) {
        int numUnits = runtimeLayout.getLayout().getSegmentLength(globalAddress);
        log.trace("Read[{}]: chain {}/{}", globalAddress, numUnits, numUnits);
        // In chain replication, we read from the last unit, though we can optimize if we
        // know where the committed tail is.
        ILogData peekResult = CFUtils.getUninterruptibly(runtimeLayout
                .getLogUnitClient(globalAddress, numUnits - 1)
                .read(globalAddress)).getAddresses().get(globalAddress);

        return peekResult.isEmpty() ? null : peekResult;
    }

    /**
     * Reads a list of global addresses from the chain of log unit servers.
     * <p>
     * - This method optimizes for the time to wait to hole fill in case empty addresses
     * are encountered.
     * - If the waitForWrite flag is set to true, when an empty address is encountered,
     * it waits for one hole to be filled. All the rest empty addresses within the list
     * are hole filled directly and the reader does not wait.
     * - In case the flag is set to false, none of the reads wait for write completion and
     * the empty addresses are hole filled right away.
     *
     * @param runtimeLayout runtime layout.
     * @param addresses     a collection of addresses to read.
     * @param waitForWrite  flag whether wait for write is required or hole fill directly.
     * @param cacheOnServer flag whether the fetch results should be cached on log unit server.
     * @return Map of read addresses.
     */
    @Override
    @Nonnull
    public Map<Long, ILogData> readAll(RuntimeLayout runtimeLayout,
                                       Collection<Long> addresses,
                                       boolean waitForWrite,
                                       boolean cacheOnServer) {

        // Group addresses by log unit client.
        Map<LogUnitClient, List<Long>> serverAddressMap =
                groupAddressByLogUnit(runtimeLayout, addresses);

        // Send read requests to log unit servers in parallel.
        List<CompletableFuture<ReadResponse>> futures = serverAddressMap
                .entrySet()
                .stream()
                .map(entry -> entry.getKey().read(entry.getValue(), cacheOnServer))
                .collect(Collectors.toList());

        // Merge the read responses from different log unit servers.
        Map<Long, LogData> readResult = futures.stream()
                .map(future -> CFUtils.getUninterruptibly(future).getAddresses())
                .reduce(new HashMap<>(), (map1, map2) -> {
                    map1.putAll(map2);
                    return map1;
                });

        return waitOrHoleFill(runtimeLayout, readResult, waitForWrite);
    }

    /**
     * Commit the addresses by first reading and then hole filling if data not existed.
     *
     * @param runtimeLayout the RuntimeLayout stamped with layout to use for commit
     * @param addresses     a collection of addresses to commit
     */
    @Override
    public void commitAll(RuntimeLayout runtimeLayout, Collection<Long> addresses) {
        // Group addresses by log unit client.
        Map<LogUnitClient, List<Long>> serverAddressMap =
                groupAddressByLogUnit(runtimeLayout, addresses);

        // Send inspect addresses requests to log unit servers in parallel.
        List<CompletableFuture<InspectAddressesResponse>> futures = serverAddressMap
                .entrySet()
                .stream()
                .map(entry -> entry.getKey().inspectAddresses(entry.getValue()))
                .collect(Collectors.toList());

        // Merge the inspect responses from different log unit servers.
        List<Long> holes = futures.stream()
                .flatMap(future -> CFUtils.getUninterruptibly(future).getEmptyAddresses().stream())
                .collect(Collectors.toList());

        // Fill all the holes in batches.
        batchHoleFill(runtimeLayout, holes);
    }

    private Map<LogUnitClient, List<Long>> groupAddressByLogUnit(RuntimeLayout runtimeLayout,
                                                                 Collection<Long> addresses) {
        // A map of log unit client to addresses it's responsible for.
        Map<LogUnitClient, List<Long>> serverAddressMap = new HashMap<>();

        for (long address : addresses) {
            int numUnits = runtimeLayout.getLayout().getSegmentLength(address);
            LogUnitClient client = runtimeLayout.getLogUnitClient(address, numUnits - 1);
            List<Long> addressList = serverAddressMap.computeIfAbsent(client, s -> new ArrayList<>());
            addressList.add(address);
        }

        return serverAddressMap;
    }

    private Map<Long, ILogData> waitOrHoleFill(RuntimeLayout runtimeLayout,
                                               Map<Long, LogData> readResult,
                                               boolean waitForWrite) {
        Map<Long, ILogData> returnResult = new HashMap<>(readResult);

        // If waiting not required for writer, hole fill directly
        if (!waitForWrite) {
            readResult.forEach((address, value) -> {
                if (value.isEmpty()) {
                    holeFill(runtimeLayout, address);
                    returnResult.put(address, peek(runtimeLayout, address));
                }
            });
            return returnResult;
        }

        // In case of holes, use the standard backoff policy for hole fill for
        // the first entry in the list. All subsequent holes in the list can be
        // hole filled without waiting as we have already waited for the first hole.
        boolean wait = true;
        for (Map.Entry<Long, LogData> entry : readResult.entrySet()) {
            long address = entry.getKey();
            ILogData value = entry.getValue();
            if (value.isEmpty()) {
                if (wait) {
                    value = read(runtimeLayout, address);
                    // Next iteration can directly read and hole fill, as we've already
                    // fulfilled the read through the hole fill policy (back-off / wait for write)
                    wait = false;
                } else {
                    // Try to read the value again because after the initial waiting,
                    // the rest of unfilled addresses might be written.
                    value = peek(runtimeLayout, address);
                    // If value is still empty, fill the hole and get the value.
                    if (value == null) {
                        holeFill(runtimeLayout, address);
                        value = peek(runtimeLayout, address);
                    }
                }
                returnResult.put(address, value);
            }
        }

        return returnResult;
    }

    private void batchHoleFill(RuntimeLayout runtimeLayout, Iterable<Long> holes) {
        final int holeFillBatchSize = 8;
        Iterable<List<Long>> batches = Iterables.partition(holes, holeFillBatchSize);

        for (List<Long> batch : batches) {
            List<CompletableFuture<Void>> futures = batch
                    .stream()
                    .map(hole -> CompletableFuture.runAsync(() -> holeFill(runtimeLayout, hole)))
                    .collect(Collectors.toList());
            futures.forEach(CFUtils::getUninterruptibly);
        }
    }

    /**
     * Propagate a write down the chain, ignoring
     * any overwrite errors. It is expected that the
     * write has already successfully completed at
     * the head of the chain.
     *
     * @param runtimeLayout the epoch stamped client containing the layout to use for propagation.
     * @param globalAddress the global address to start writing at.
     * @param data          the data to propagate, or NULL, if it is to be a hole.
     */
    private void propagate(RuntimeLayout runtimeLayout,
                           long globalAddress,
                           @Nullable ILogData data) {
        int numUnits = runtimeLayout.getLayout().getSegmentLength(globalAddress);
        for (int i = 1; i < numUnits; i++) {
            log.trace("Propagate[{}]: chain {}/{}", Token.of(runtimeLayout.getLayout().getEpoch(),
                    globalAddress),
                    i + 1, numUnits);
            // In chain replication, we write synchronously to every unit
            // in the chain.
            try {
                Runnable writable;
                final int writableIndex = i;
                if (data != null) {
                    writable = () -> CFUtils.getUninterruptibly(
                            runtimeLayout.getLogUnitClient(globalAddress, writableIndex)
                                    .write(data),
                            OverwriteException.class);
                } else {
                    Token token = new Token(runtimeLayout.getLayout().getEpoch(), globalAddress);
                    LogData hole = LogData.getHole(token);
                    writable = () -> CFUtils.getUninterruptibly(runtimeLayout
                            .getLogUnitClient(globalAddress, writableIndex)
                            .write(hole), OverwriteException.class);
                }
                doWrite(runtimeLayout, globalAddress, i, writable);
            } catch (OverwriteException oe) {
                log.info("Propagate[{}]: Completed by other writer", globalAddress);
            }
        }
    }

    /**
     * Recover a failed write at the given global address,
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
     * @param runtimeLayout the RuntimeLayout to use for the recovery.
     * @param globalAddress the global address to drive the recovery protocol
     */
    private void recover(RuntimeLayout runtimeLayout, long globalAddress) {
        final Layout layout = runtimeLayout.getLayout();
        // In chain replication, we started writing from the head,
        // and propagated down to the tail. To recover, we start
        // reading from the head, which should have the data
        // we are trying to recover
        int numUnits = layout.getSegmentLength(globalAddress);
        log.warn("Recover[{}]: read chain head {}/{}", Token.of(runtimeLayout.getLayout().getEpoch()
                , globalAddress),
                1, numUnits);
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
            final int writableIndex = i;
            // In chain replication, we write synchronously to every unit in the chain.
            try {
                Runnable writable = () -> CFUtils.getUninterruptibly(
                        runtimeLayout.getLogUnitClient(globalAddress, writableIndex).write(ld),
                        OverwriteException.class);
                doWrite(runtimeLayout, globalAddress, writableIndex, writable);
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
    protected void holeFill(RuntimeLayout runtimeLayout, long globalAddress) {
        int numUnits = runtimeLayout.getLayout().getSegmentLength(globalAddress);
        log.warn("fillHole[{}]: chain head {}/{}", Token.of(runtimeLayout.getLayout().getEpoch(), globalAddress),
                1, numUnits);
        // In chain replication, we write synchronously to every unit in
        // the chain.
        try {
            Token token = new Token(runtimeLayout.getLayout().getEpoch(), globalAddress);
            LogData hole = LogData.getHole(token);
            final int headIndex = 0;
            Runnable writable = () -> CFUtils.getUninterruptibly(runtimeLayout
                    .getLogUnitClient(globalAddress, headIndex)
                    .write(hole), OverwriteException.class);
            doWrite(runtimeLayout, globalAddress, headIndex, writable);
            propagate(runtimeLayout, globalAddress, null);
        } catch (OverwriteException oe) {
            // The hole-fill failed. We must ensure the other writer's
            // value is adopted before returning.
            recover(runtimeLayout, globalAddress);
        }
    }
}

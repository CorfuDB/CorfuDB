package org.corfudb.runtime;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.AddressRequest;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The address loader is the layer responsible for loading all addresses from the log unit(s).
 *
 * It provides controlled access to the space of shared addresses for multiple threads.
 */
@Slf4j
public class AddressLoader implements Runnable {
    // Pending Reads Queue
    volatile BlockingQueue<AddressRequest> pendingReadsQueue;

    // Read Function
    volatile Function<Set<Long>, Map<Long, ILogData>> readFunction;

    Cache<Long, ILogData> readCache;

    volatile boolean shutdown = false;

    final int batchSize = 10;
    final int numReaders = 4;

    /**
     * To dispatch tasks for failure or healed nodes detection.
     */
    @Getter
    private final ExecutorService addressLoaderFetchWorker;

    public AddressLoader(Function<Set<Long>, Map<Long, ILogData>> readFunction, Cache<Long, ILogData> readCache) {
        // TODO: should it be bounded?
        this.pendingReadsQueue = new LinkedBlockingQueue<>();
        this.readFunction = readFunction;
        this.addressLoaderFetchWorker = Executors.newFixedThreadPool(
                numReaders,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("fetcher-%d")
                        .build()
        );

        this.readCache = readCache;
    }

    /**
     * Enqueue addresses to read in order to be consumed by the address loader fetcher.
     *
     * @param addresses list of addresses to be read.
     * @return map of addresses to log data.
     */
    public synchronized CompletableFuture<Map<Long, ILogData>> enqueueReads(List<Long> addresses) {

        Set<AddressRequest> requests = new HashSet<>();

        // Create an address request per address with its own Completable Future and add to pending read queue.
        addresses.forEach(address -> {
            CompletableFuture<ILogData> addressCF = new CompletableFuture<>();
            AddressRequest request = new AddressRequest(address, addressCF);
            requests.add(request);
            this.pendingReadsQueue.add(request);
        });

        // Create a composed Completable Future, which completes when all address requests complete / return a map
        CompletableFuture<Map<Long, ILogData>> cf = CompletableFuture.allOf(requests.stream()
                .map(request -> request.getCf())
                .toArray(CompletableFuture[]::new))
                .thenApply(avoid -> requests.stream().collect(Collectors.toMap(AddressRequest::getAddress, addressRequest -> {
                    try {
                        return addressRequest.getCf().get();
                    } catch (InterruptedException ie) {
                        throw new UnrecoverableCorfuInterruptedError(ie);
                    } catch (Exception e) {
                        throw (RuntimeException) e;
                    }
                })));

        return cf;
    }

    @Override
    public void run() {
        this.batcher();
    }

    // Batcher: generate batches of read addresses to be sent to consumer threads
    // this is required to maintain unique elements in the queue (avoid reading the same
    // address repeatedly)
    public void batcher() {

        Map<Long, List<CompletableFuture<ILogData>>> currentBatches = new HashMap<>();

        try {
            long counter = 0;
            Set<Long> fetchBatch = new HashSet<>();
            AddressRequest lastRead = null;

            while (!shutdown) {

                AddressRequest read;
                if (lastRead == null && futures.isEmpty()) {
                    read = pendingReadsQueue.take();
                } else {
                    // Get read request from queue
                    read = pendingReadsQueue.poll();
                }

                if (read != null) {
                    // Because we are not guaranteeing uniqueness between cycles, check if read was
                    // serviced in previous cycle
                    ILogData readValue = this.readCache.getIfPresent(read);
                    if (readValue == null) {
                        if (!currentBatches.containsKey(read.getAddress())) {
                            counter++;
                            currentBatches.compute(read.getAddress(), (address, value) -> {
                                // The value is always null, as we already verified the key is not present
                                List<CompletableFuture<ILogData>> cfs = new ArrayList<>();
                                cfs.add(read.getCf());
                                return cfs;
                            });

                            fetchBatch.add(read.getAddress());
                            if (counter % batchSize == 0) {
                                final Set<Long> s1 = new HashSet<>(fetchBatch);
                                futures.put(CompletableFuture
                                        .supplyAsync(() -> fetch(s1), addressLoaderFetchWorker), new HashSet<>(fetchBatch));
                                fetchBatch.clear();
                            }
                        } else {
                            currentBatches.compute(read.getAddress(), (address, value) -> {
                                    value.add(read.getCf());
                                    return value;
                                });
                        }
                    } else {
                        // The read is already available in the cache, service the read, i.e., complete the future.
                        read.getCf().complete(readValue);
                    }
                } else {
                    if (!fetchBatch.isEmpty()) {
                        final Set<Long> s2 = new HashSet<>(fetchBatch);
                        futures.put(CompletableFuture
                                .supplyAsync(() -> fetch(s2), addressLoaderFetchWorker), new HashSet<>(fetchBatch));
                        fetchBatch.clear();
                    }
                }

                // If any future (pending response from reader) is present, check for completeness
                if (!futures.isEmpty()) {
                    Set<Long> completedReads = checkIfAnyReaderCompleted(currentBatches);
                    completedReads.forEach(address -> currentBatches.remove(address));
                }
                lastRead = read;
            }
        } catch (InterruptedException ie) {
            throw new UnrecoverableCorfuInterruptedError(ie);
        } catch (Throwable t) {
            log.error("Batcher errpr: ", t);
            throw t;
        }
    }

    private Set<Long> checkIfAnyReaderCompleted(Map<Long, List<CompletableFuture<ILogData>>> currentBatches) {
        try {

            boolean anyIsDone = false;
            for(CompletableFuture<Map<Long, ILogData>> cf : futures.keySet()) {
                if (cf.isDone()) {
                    anyIsDone = true;
                    break;
                }
            }

            if (!anyIsDone) {
                if (futures.size() != numReaders) {
                    return Collections.emptySet();
                } else {
                    CompletableFuture cfAnyOf = CompletableFuture.anyOf(futures.keySet().toArray(new CompletableFuture[futures.size()]));
                    cfAnyOf.get();
                }
            }
        } catch (Exception ignore) {
            // Ignore all exceptions, these will be taken care at the read request level next
           log.error("Error in check completable futures: ", ignore);
        }

        final Set<Long> completedReads = new HashSet<>();

        this.futures = futures.entrySet().stream()
                .filter(entry -> {
                    Map<Long, ILogData> dataMap;
                    boolean completed = false;
                    try {
                        dataMap = entry.getKey().getNow(null);
                    } catch (Exception e) {
                        // Case: completed exceptionally , we need to remove these CFs
                        entry.getValue().forEach(address -> currentBatches.get(address).forEach(cf -> cf.completeExceptionally(e)));
                        return false;
                    }
                    if (dataMap != null) {
                        completed = true;
                        dataMap.forEach((a, v) -> currentBatches.get(a).forEach(cf -> cf.complete(v)));
                        completedReads.addAll(dataMap.keySet());
                    }
                    return !completed;
                })
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        log.trace("AddressLoader: completed reads: {}", completedReads);
        return completedReads;
    }

    private volatile Map<CompletableFuture<Map<Long, ILogData>>, Set<Long>> futures = new HashMap<>();

    // Running in a background thread
    @SuppressWarnings({"checkstyle:printLine", "checkstyle:printStackTrace"})
    public Map<Long, ILogData> fetch(Set<Long> batch) {
        try {
            return this.readFunction.apply(batch);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    public void stop() {
        shutdown = true;
        this.addressLoaderFetchWorker.shutdownNow();
    }
}
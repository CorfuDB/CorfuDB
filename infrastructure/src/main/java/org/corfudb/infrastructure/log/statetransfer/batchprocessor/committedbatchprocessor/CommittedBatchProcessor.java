package org.corfudb.infrastructure.log.statetransfer.batchprocessor.committedbatchprocessor;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequestForNode;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.exceptions.ReadBatchException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferBatchProcessorException;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static lombok.Builder.Default;
import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse.TransferStatus.FAILED;

/**
 * A transferBatchRequest processor that transfers committed addresses one transferBatchRequest
 * at a time via reading directly from the provided log unit servers in a random order.
 * A committed address is the one that belongs to the part of an address space that has been
 * previously committed by the auto-commit service or some previously run state transfer.
 */
@Slf4j
@Builder
@Getter
public class CommittedBatchProcessor implements StateTransferBatchProcessor {

    /**
     * Configurations for the retry logic.
     */
    @Default
    private final int maxReadRetries = 3;
    @Default
    private final Duration readSleepDuration = Duration.ofMillis(300);
    @Default
    private final int maxWriteRetries = 3;
    @Default
    private final Duration writeSleepDuration = Duration.ofMillis(300);

    /**
     * Current node.
     */
    private final String currentNode;
    /**
     * Current corfu runtime layout.
     */
    private final RuntimeLayout runtimeLayout;


    /**
     * An iterator over a provided transfer batch request. It iterates over the available
     * destination nodes in the random order producing the TransferBatchRequestForNode requests.
     * This allows to distribute the workload among the multiple consecutive transfer batch
     * requests destined for this batch processor somewhat evenly.
     */
    public static class RandomNodeIterator implements Iterator<TransferBatchRequestForNode> {

        private final List<Long> originalAddresses;
        private final Iterator<String> iter;

        public RandomNodeIterator(TransferBatchRequest request) {
            originalAddresses = request.getAddresses();
            ArrayList<String> nodes = new ArrayList<>(request.getDestinationNodes()
                    .orElse(ImmutableList.of()));
            Collections.shuffle(nodes);
            iter = nodes.iterator();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public TransferBatchRequestForNode next() {
            String nextNode = iter.next();
            return new TransferBatchRequestForNode(originalAddresses, nextNode);
        }
    }

    /**
     * Given a random node iterator created from the transfer batch request, try perform a
     * state transfer by reading the data from the randomly picked log unit and writing it to
     * the current log unit server. If the read failed for a node, pick the next available one
     * randomly and retry. If the transfer failed for some other reason (e.g. during the writes) or
     * if there are no more nodes left to distribute the workload over,
     * fail the transfer with an exception.
     *
     * @param nodeIterator A random node iterator produced from the transfer batch request.
     * @return A normally completed future if the transfer for this batch succeeds, an exceptionally
     * completed future, otherwise.
     */
    private CompletableFuture<TransferBatchResponse> tryTransferForRandomNodes(
            RandomNodeIterator nodeIterator) {
        return CompletableFuture.supplyAsync(() -> {
            while (nodeIterator.hasNext()) {
                TransferBatchRequestForNode transferBatchRequestForNode = nodeIterator.next();
                List<Long> addresses = transferBatchRequestForNode.getAddresses();
                String destinationNode = transferBatchRequestForNode.getDestinationNode();
                LogUnitClient logUnitClientToTargetNode =
                        runtimeLayout.getLogUnitClient(destinationNode);
                LogUnitClient logUnitClientToCurrentNode =
                        runtimeLayout.getLogUnitClient(currentNode);
                try {
                    ReadBatch readBatch = readRecords(addresses,
                            Optional.of(destinationNode), logUnitClientToTargetNode);
                    return writeRecords(readBatch, logUnitClientToCurrentNode,
                            maxWriteRetries, writeSleepDuration);
                } catch (ReadBatchException re) {
                    log.warn("Read exception for node {} occurred. " +
                            "Retry transfer for the next available node.", destinationNode, re);
                } catch (Exception e) {
                    log.error("Exception during batch transfer occurred. Failing the transfer.", e);
                    throw e;
                }
            }
            throw new ReadBatchException("No target nodes left to select from");
        });
    }

    @Override
    public CompletableFuture<TransferBatchResponse> transfer(
            TransferBatchRequest transferBatchRequest) {
        return tryTransferForRandomNodes(new RandomNodeIterator(transferBatchRequest))
                .exceptionally(error -> TransferBatchResponse
                        .builder()
                        .transferBatchRequest(transferBatchRequest)
                        .status(FAILED)
                        .causeOfFailure(Optional.of(new StateTransferBatchProcessorException(
                                "Failed batch: " + transferBatchRequest, error)))
                        .build()
                );
    }

    /**
     * Read records directly from the randomly scheduled destination node (don't hole fill).
     *
     * @param addresses A batch of consecutive addresses.
     * @param destNode  An optional destination node.
     * @param client    A log unit client to the node.
     * @return A read batch if the read went fine, an exception otherwise.
     */
    public ReadBatch readRecords(List<Long> addresses,
                                 Optional<String> destNode,
                                 LogUnitClient client) {
        for (int i = 0; i < maxReadRetries; i++) {
            try {
                ReadResponse response = CFUtils
                        .getUninterruptibly(client.readAll(addresses));
                Map<Long, ILogData> records = new HashMap<>(response.getAddresses());
                ReadBatch readBatch = checkReadRecords(addresses,
                        records, destNode);
                if (readBatch.getStatus() == ReadBatch.ReadStatus.FAILED) {
                    throw new IllegalStateException("Some addresses failed to transfer: " +
                            readBatch.getFailedAddresses());
                }
                return readBatch;

            } catch (WrongEpochException e) {
                // If the WEE is hit, fail immediately.
                log.warn("readRecords: encountered a wrong epoch exception on try {}: {}.",
                        i, e);
                throw e;

            } catch (RuntimeException e) {
                log.warn("readRecords: encountered an exception on try {}: {}.", i, e);
                Sleep.sleepUninterruptibly(readSleepDuration);
            }
        }
        // If the retries are exhausted, throw ReadBatchException.
        throw new ReadBatchException("readRecords: read retries were exhausted");
    }

}

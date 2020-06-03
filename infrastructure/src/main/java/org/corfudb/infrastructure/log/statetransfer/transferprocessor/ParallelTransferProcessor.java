package org.corfudb.infrastructure.log.statetransfer.transferprocessor;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferBatchProcessorException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.TransferConsumerException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.TransferProducerException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.TransferSegmentException;
import org.corfudb.util.CFUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest.TransferBatchType.DATA;
import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest.TransferBatchType.POISON_PILL;
import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse.TransferStatus.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.transferprocessor.TransferProcessor.TransferProcessorResult.TransferProcessorStatus.TRANSFER_FAILED;
import static org.corfudb.infrastructure.log.statetransfer.transferprocessor.TransferProcessor.TransferProcessorResult.TransferProcessorStatus.TRANSFER_SUCCEEDED;

/**
 * A transfer processor that performs state transfer by distributing
 * the workload among the multiple batch processor consumer tasks via a blocking queue.
 */
@Slf4j
@Getter
public class ParallelTransferProcessor implements TransferProcessor {
    /**
     * A blocking queue for the producer-consumer interaction.
     */
    private final BlockingQueue<TransferBatchRequest> queue;
    /**
     * A time in seconds it takes for a producer to timeout while waiting to offer a next data batch
     * or a poison pill message if the queue is full.
     */
    private final long offerTimeOutInSeconds;
    /**
     * A time in seconds it takes for a consumer to timeout after waiting to poll the next data batch
     * or a poison pill message if the queue is empty.
     */
    private final long pollTimeOutInSeconds;
    /**
     * Units for the timeouts.
     */
    private final TimeUnit units = TimeUnit.SECONDS;
    /**
     * A configured batch processor that handles the transfer of a batch of addresses
     * (via direct read or a protocol), as well as all the required retry and error handling logic
     * during it.
     */
    private final StateTransferBatchProcessor batchProcessor;
    /**
     * A number of competing consumers that perform the batch state transfer in parallel.
     */
    private final int numConsumers;

    public ParallelTransferProcessor(int queueSize, StateTransferBatchProcessor committedBatchProcessor) {
        numConsumers = queueSize;
        queue = new LinkedBlockingDeque<>(queueSize);
        batchProcessor = committedBatchProcessor;
        offerTimeOutInSeconds = 10L;
        pollTimeOutInSeconds = 2L;
    }

    public ParallelTransferProcessor(int queueSize,
                                     StateTransferBatchProcessor committedBatchProcessor,
                                     long offerTimeOutInSeconds, long pollTimeOutInSeconds) {
        numConsumers = queueSize;
        queue = new LinkedBlockingDeque<>(queueSize);
        batchProcessor = committedBatchProcessor;
        this.offerTimeOutInSeconds = offerTimeOutInSeconds;
        this.pollTimeOutInSeconds = pollTimeOutInSeconds;
    }

    /**
     * Poll the blocking queue to get the next transfer batch request.
     * If the poll times out, finish the current task with a TimeoutException.
     * If the request is retrieved, and its of type DATA - transfer it with a batch processor.
     * If this batch processor fails to transfer a batch, fail the current task with the
     * offending exception.
     * If the request is retrieved and its of a type POISON_PILL - finish this
     * task gracefully.
     * Otherwise - throw an exception.
     *
     * @return A completed future if all the DATA requests has been processed
     * and the POISON_PILL message is received, an exceptionally completed
     * future otherwise.
     */
    private CompletableFuture<Void> runConsumer() {
        return CompletableFuture.runAsync(() -> {
            boolean transferInProgress = true;
            while (transferInProgress) {
                try {
                    // Wait up to pollTimeOutInSeconds seconds to get the next message.
                    Optional<TransferBatchRequest> maybeRequest =
                            Optional.ofNullable(queue.poll(pollTimeOutInSeconds, units));
                    if (maybeRequest.isPresent() &&
                            maybeRequest.get().getTransferBatchType() == DATA) {
                        // Got a data request.
                        // Perform a transfer with a given batch processor.
                        // If transfer fails, complete exceptionally.
                        TransferBatchResponse response = batchProcessor.transfer(maybeRequest.get())
                                .join();
                        if (response.getStatus() == FAILED) {
                            String errorMsg = String.format("Failed request %s",
                                    response.getTransferBatchRequest());
                            StateTransferBatchProcessorException ex = response
                                    .getCauseOfFailure()
                                    .orElse(new StateTransferBatchProcessorException(errorMsg));
                            log.error("runConsumer: consumer task finished with error:", ex);
                            throw ex;
                        }
                    } else if (maybeRequest.isPresent() &&
                            maybeRequest.get().getTransferBatchType() == POISON_PILL) {
                        // Got a poison pill message, finish gracefully.
                        log.debug("runConsumer: consumer task finished successfully.");
                        transferInProgress = false;
                    } else if (maybeRequest.isPresent()) {
                        log.error("runConsumer: Unrecognized message: {}.",
                                maybeRequest.get().getTransferBatchType());
                        throw new IllegalStateException("Message " + maybeRequest.get() +
                                " is unrecognized.");
                    } else {
                        throw new TimeoutException("runConsumer: consumer timed out " +
                                "polling a message.");
                    }

                } catch (InterruptedException ie) {
                    log.error("runConsumer: consumer task was interrupted.");
                    Thread.currentThread().interrupt();
                    throw new TransferConsumerException(ie);
                } catch (TimeoutException | IllegalStateException e) {
                    throw new TransferConsumerException(e);
                }
            }
        });
    }

    /**
     * Send numConsumers poison pill messages to the queue to shutdown the consumers gracefully.
     *
     * @param numConsumers The number of consumers waiting on the other end of the queue.
     * @throws InterruptedException If the current thread is interrupted.
     * @throws TimeoutException     If times out while offering a pill to the queue.
     */
    private void finishConsumers(int numConsumers) throws InterruptedException, TimeoutException {
        log.debug("finishConsumers: producer finishing consumer tasks.");
        for (int i = 0; i < numConsumers; i++) {
            TransferBatchRequest poisonPill =
                    TransferBatchRequest.builder().transferBatchType(POISON_PILL).build();
            if (!queue.offer(poisonPill, offerTimeOutInSeconds, units)) {
                throw new TimeoutException("finishConsumers: producer timed" +
                        " out waiting to offer a poison pill message.");
            }
        }
    }

    /**
     * Get the next transfer batch request and offer it to the blocking queue.
     * Once done, finish the transfer by sending the poison pill messages.
     *
     * @param batchStreamIterator A stream of transfer batch requests.
     * @param numConsumers        A number of consumers on the other end of the queue.
     * @return A completable future that completes successfully if the stream is processed
     * and all the poison pill messages are sent, an exceptionally completed future otherwise.
     */
    CompletableFuture<Void> runProducer(Iterator<TransferBatchRequest> batchStreamIterator,
                                        int numConsumers) {
        return CompletableFuture.runAsync(() -> {
            try {
                while (batchStreamIterator.hasNext()) {
                    TransferBatchRequest request = batchStreamIterator.next();
                    if (!queue.offer(request, offerTimeOutInSeconds, units)) {
                        throw new TimeoutException("runProducer: producer timed" +
                                " out waiting to offer a batch.");
                    }
                }
                finishConsumers(numConsumers);

            } catch (InterruptedException ie) {
                log.error("runProducer: producer interrupted.");
                Thread.currentThread().interrupt();
                throw new TransferProducerException(ie);
            } catch (TimeoutException toe) {
                log.error("runProducer: producer could not offer a payload within " +
                        "a given time range.");
                throw new TransferProducerException(toe);
            }
        });
    }

    /**
     * Run multiple consumers and one producer tasks, performing the batch transfer
     * for each of the requests
     * in the stream. Only when all the tasks are completed successfully,
     * return a TransferProcessorResult of type TRANSFER_SUCCEEDED,
     * otherwise return a TransferProcessorResult of type TRANSFER_FAILED.
     *
     * @param batchStream A stream of batch requests.
     * @return A transfer processor result with a state TRANSFER_SUCCEEDED if
     * all tasks finish successfully,
     * a transfer processor with a state TRANSFER_FAILED and the cause of failure otherwise.
     */
    @Override
    public CompletableFuture<TransferProcessorResult> runStateTransfer(
            Stream<TransferBatchRequest> batchStream) {

        // Get a transfer batch request stream iterator
        Iterator<TransferBatchRequest> iterator = batchStream.iterator();

        // No need to spawn tasks if there is no work to do.
        if (!iterator.hasNext()) {
            return CompletableFuture.completedFuture(TransferProcessorResult.builder()
                    .transferState(TRANSFER_SUCCEEDED).build());
        }

        // Run number of consumer tasks
        List<CompletableFuture<Void>> consumerTasksToFutureList = IntStream.range(0, numConsumers)
                .boxed()
                .map(i -> runConsumer()).collect(Collectors.toList());

        // Run a producer task
        CompletableFuture<Void> producerTaskFuture
                = runProducer(iterator, numConsumers);

        // Sequence the consumer tasks
        CompletableFuture<List<Void>> consumersTaskFuture =
                CFUtils.sequence(consumerTasksToFutureList);

        // Aggregate all the tasks
        CompletableFuture<Void> allTasks =
                CompletableFuture.allOf(producerTaskFuture, consumersTaskFuture);

        // If either the producer or one or more consumer tasks fail -> fail all the tasks.
        Stream.of(producerTaskFuture, consumersTaskFuture)
                .forEach(future -> future.exceptionally(e -> {
                    allTasks.completeExceptionally(e);
                    return null;
                }));

        // Handle result
        return allTasks.handle((res, ex) -> {
            if (ex == null) {
                return TransferProcessorResult.builder().transferState(TRANSFER_SUCCEEDED).build();
            }
            queue.clear();
            return TransferProcessorResult
                    .builder()
                    .transferState(TRANSFER_FAILED)
                    .causeOfFailure(Optional.of(new TransferSegmentException(ex)))
                    .build();
        });
    }
}

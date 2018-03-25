package org.corfudb.runtime.view;

import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Data;
import org.corfudb.protocols.wireprotocol.BatchTokenRequest;
import org.corfudb.protocols.wireprotocol.BatchTokenResponse;
import org.corfudb.protocols.wireprotocol.BatchTokenResponse.BackpointerToken;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TokenType;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;

public class BatchSequencerView extends SequencerView {

    @Data
    private static class BatchSequencerOperation {

        final boolean next;

        final TxResolutionInfo info;

        final UUID[] streams;

        final CompletableFuture<TokenResponse> completion = new CompletableFuture<>();

        TokenResponse response = null;

        void completeResponse() {
            if (response == null) {
                throw new UnrecoverableCorfuError("Attempted to complete an empty token");
            }
            completion.complete(response);
        }
    }

    Semaphore s = new Semaphore(0);

    final Deque<BatchSequencerOperation> queue = new ConcurrentLinkedDeque<>();

    final ExecutorService batcher = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()                                                  .setNameFormat("sequencer-batcher-%d").build());


    public BatchSequencerView(CorfuRuntime runtime) {
        super(runtime);
        batcher.submit(this::sequencerBatcher);
    }

    private void sequencerBatcher() {
        while (!runtime.isShutdown()) {
            // A list of completed operations
            List<BatchSequencerOperation> abortedOps = new ArrayList<>();
            List<BatchSequencerOperation> readOps = new ArrayList<>();
            List<BatchSequencerOperation> writeOps = new ArrayList<>();
            List<BatchSequencerOperation> conditionalWriteOps = new ArrayList<>();

            Set<UUID> readStreams = new HashSet<>();
            List<UUID[]> tokenRequests = new ArrayList<>();
            List<TxResolutionInfo> conditionalTokenRequests = new ArrayList<>();
            SetMultimap<UUID, ByteBuffer> conflictMap =
                MultimapBuilder.hashKeys().hashSetValues().build();
            try {
                s.tryAcquire(4, 1, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                throw new UnrecoverableCorfuInterruptedError(ie);
            }
            while (queue.size() > 0) {
                BatchSequencerOperation op = queue.pollFirst();
                if (op == null) {
                    break;
                }
                if (op.next) {
                    if (op.info == null) {
                        tokenRequests.add(op.streams);
                        writeOps.add(op);
                    } else {
                        boolean isLocallyConflicted = false;
                        for (Entry<UUID, Set<byte[]>> e : op.info.getConflictSet().entrySet()) {
                            Set<ByteBuffer> conflictSet = e.getValue().stream()
                                .map(ByteBuffer::wrap).collect(Collectors.toSet());
                            Set<ByteBuffer> written = conflictMap.get(e.getKey());
                            Set<ByteBuffer> conflicts = Sets.intersection(written, conflictSet);
                            if (!conflicts.isEmpty()) {
                                isLocallyConflicted = true;
                                op.response = new TokenResponse(TokenType.TX_ABORT_CONFLICT,
                                    conflicts.iterator().next().array(),
                                    new Token(Address.ABORTED,
                                        0), Collections.emptyMap()
                                    );
                                abortedOps.add(op);
                                break;
                            } else {
                                conflictMap.putAll(e.getKey(), conflictSet);
                            }
                        }
                        if (!isLocallyConflicted) {
                            conditionalTokenRequests.add(op.info);
                            conditionalWriteOps.add(op);
                        }
                    }
                } else {
                    // Batch reads
                    readOps.add(op);
                    readStreams.addAll(Arrays.asList(op.streams));
                }
            }

            if (readOps.size() != 0 || writeOps.size() != 0 || conditionalWriteOps.size() != 0) {
                BatchTokenRequest r = new BatchTokenRequest(readStreams.toArray(new UUID[0]),
                    tokenRequests, conditionalTokenRequests);
                try {
                    BatchTokenResponse response =
                        layoutHelper(l -> l.getPrimarySequencerClient().getBatchToken(r).join());
                    // Handle a batched read
                    readOps.forEach(op -> {
                        if (op.streams.length == 0) {
                            op.response
                                = new TokenResponse(response.getGlobalTail(),
                                response.getEpoch(),
                                Collections.emptyMap());
                        } else {
                            op.response
                                = new TokenResponse(response
                                .getAddressMap().get(op.streams[0]),
                                response.getEpoch(), Collections.emptyMap());
                        }
                        op.completeResponse();
                    });

                    for (int i = 0; i < response.getUnconditionalTokens().size(); i++) {
                        final BackpointerToken token = response.getUnconditionalTokens().get(i);
                        writeOps.get(i).response = new TokenResponse(token.getToken(),
                            response.getEpoch(),
                            token.getBackpointerMap());
                        writeOps.get(i).completeResponse();
                    }

                    for (int i = 0; i < response.getConditionalTokens().size(); i++) {
                        final BackpointerToken token = response.getConditionalTokens().get(i);
                        if (token.getToken() == Address.ABORTED) {
                            conditionalWriteOps.get(i).response = new TokenResponse(TokenType.TX_ABORT_CONFLICT,
                                new byte[0],
                                new Token(Address.ABORTED,
                                    0), Collections.emptyMap()
                            );
                        } else {
                            conditionalWriteOps.get(i).response = new TokenResponse(
                                token.getToken(),
                                response.getEpoch(),
                                token.getBackpointerMap());
                        }
                        conditionalWriteOps.get(i).completeResponse();
                    }
                } catch (CompletionException ce) {
                    queue.addAll(readOps);
                    queue.addAll(writeOps);
                    queue.addAll(conditionalWriteOps);
                }
            }

            abortedOps.forEach(BatchSequencerOperation::completeResponse);
        }
    }

    private CompletableFuture<TokenResponse> queueBatchOperation(boolean next,
                                                                 @Nullable  TxResolutionInfo info,
                                                                 UUID... streams) {
        if (runtime.isShutdown()) {
            throw new UnrecoverableCorfuError("Runtime is shutdown");
        }
        s.release();
        BatchSequencerOperation op = new BatchSequencerOperation(next, info, streams);
        queue.add(op);
        return op.completion;
    }

    @Override
    public TokenResponse nextConditionalToken(@Nullable TxResolutionInfo info, UUID... streams) {
        return queueBatchOperation(true, info, streams).join();
    }

    @Override
    public TokenResponse nextToken(UUID... streams) {
        return queueBatchOperation(true, null, streams).join();
    }

    @Override
    public TokenResponse currentToken(UUID... streams) {
        return queueBatchOperation(false, null, streams).join();
    }


}
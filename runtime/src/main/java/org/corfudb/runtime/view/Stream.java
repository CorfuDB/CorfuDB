package org.corfudb.runtime.view;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.ServerNotReadyException;
import org.corfudb.runtime.exceptions.WrongEpochException;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Stream {

    ScheduledExecutorService executorService;

    final UUID id;

    final CorfuRuntime rt;

    public Stream(CorfuRuntime rt, UUID streamId, String name) {
        this.rt = rt;
        this.id = streamId;

        Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("Stream-%d" + "-" + name)
                .build());
    }


    CompletableFuture<RuntimeLayout> getRuntimeLayout() {
        CompletableFuture<RuntimeLayout> future = new CompletableFuture<>();
        future.complete(rt.getLayoutView().getRuntimeLayout());
        return future;
    }

    CompletableFuture<TokenResponse> getToken(RuntimeLayout runtimeLayout, UUID id) {
        return runtimeLayout.getPrimarySequencerClient().nextToken(Collections.singletonList(id), 1);
    }

    private CompletableFuture<Boolean> write(TokenResponse tokenResponse, Object object,
                                            CompletableFuture<RuntimeLayout> layoutCompletableFuture) {
        LogData ld = new LogData(DataType.DATA, object, rt.getParameters().getCodecType());
        ld.useToken(tokenResponse);
        return layoutCompletableFuture.thenCompose(runtimeLayout ->
                runtimeLayout.getLogUnitClient(tokenResponse.getSequence(), 0).write(ld));
    }

    public CompletableFuture<Boolean> asyncAppend(Object object) {
        CompletableFuture<Boolean> finalResult = new CompletableFuture<>();

        appendAndRetry(object, finalResult);

        return finalResult;
    }

    private CompletableFuture appendAndRetry(Object object, CompletableFuture result) {
        CompletableFuture<RuntimeLayout> runtimeLayoutFuture = getRuntimeLayout();

        return runtimeLayoutFuture
                .thenCompose(runtimeLayout -> getToken(runtimeLayout, id))
                .thenCompose(tokenResponse -> write(tokenResponse, object, runtimeLayoutFuture))
                .thenAccept(res -> result.complete(res))
                .exceptionally(ex -> {
                    executorService.schedule(() -> appendAndRetry(object, result), 1000, TimeUnit.MILLISECONDS);
                    //System.out.println("Exception rescheduling " + ex);
                    return null;
                });
    }


}

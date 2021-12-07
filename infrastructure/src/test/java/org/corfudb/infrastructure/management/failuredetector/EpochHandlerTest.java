package org.corfudb.infrastructure.management.failuredetector;

import org.corfudb.infrastructure.LayoutBasedTestHelper;
import org.corfudb.infrastructure.NodeNames;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class EpochHandlerTest extends LayoutBasedTestHelper {

    @Test
    void fetchLatestLayoutEmpty() {
        Optional<Layout> latestLayout = buildHandler()
                .fetchLatestLayout(new HashMap<>())
                .join();

        assertFalse(latestLayout.isPresent());
    }

    @Test
    void fetchLatestLayoutConnectionError() {
        CompletableFuture<Layout> err = new CompletableFuture<>();
        err.completeExceptionally(new WrongEpochException(123));

        CompletableFuture<Layout> err2 = new CompletableFuture<>();
        err2.completeExceptionally(new WrongEpochException(321));

        Map<String, CompletableFuture<Layout>> futureLayouts = new HashMap<>();
        futureLayouts.put(NodeNames.A, err);
        futureLayouts.put(NodeNames.B, err2);

        Optional<Layout> latestLayout = buildHandler()
                .fetchLatestLayout(futureLayouts)
                .join();

        assertFalse(latestLayout.isPresent());
    }

    @Test
    void fetchLatestLayoutOneServer() {
        CompletableFuture<Layout> cf0 = CompletableFuture.completedFuture(buildSimpleLayout());

        HashMap<String, CompletableFuture<Layout>> futureLayouts = new HashMap<>();
        futureLayouts.put(NodeNames.A, cf0);

        Optional<Layout> latestLayout = buildHandler()
                .fetchLatestLayout(futureLayouts)
                .join();

        assertTrue(latestLayout.isPresent());
    }

    @Test
    void fetchLatestLayoutThreeNodesOneDown() {
        final int highestEpoch = 33;

        CompletableFuture<Layout> cf0 = CompletableFuture.completedFuture(buildSimpleLayout());
        CompletableFuture<Layout> cf1 = CompletableFuture.completedFuture(buildSimpleLayout(highestEpoch));
        CompletableFuture<Layout> cf2 = new CompletableFuture<>();
        cf2.completeExceptionally(new WrongEpochException(123));

        HashMap<String, CompletableFuture<Layout>> futureLayouts = new HashMap<>();
        futureLayouts.put(NodeNames.A, cf0);
        futureLayouts.put(NodeNames.B, cf1);
        futureLayouts.put(NodeNames.C, cf2);

        Optional<Layout> latestLayout = buildHandler()
                .fetchLatestLayout(futureLayouts)
                .join();

        assertTrue(latestLayout.isPresent());
        assertEquals(highestEpoch, latestLayout.get().getEpoch());
    }

    @Test
    void updateTrailingLayoutServersCommitLayoutExecution() {
        EpochHandler handlerSpy = Mockito.spy(buildHandler());

        HashMap<String, CompletableFuture<Layout>> requests = new HashMap<>();
        CompletableFuture<Layout> request1 = new CompletableFuture<>();
        request1.complete(buildSimpleLayout(2));
        requests.put(NodeNames.A, request1);

        CompletableFuture<Boolean> asyncCommit = CompletableFuture.completedFuture(true);
        doReturn(asyncCommit).when(handlerSpy).commitLayout(isA(Layout.class), anyString());

        Layout layout1 = buildSimpleLayout();
        handlerSpy.updateTrailingLayoutServers(layout1, requests);

        verify(handlerSpy, times(1)).commitLayout(any(Layout.class), anyString());
    }

    @Test
    void updateTrailingLayoutServersFailedConnections() {
        EpochHandler handlerSpy = Mockito.spy(buildHandler());

        CompletableFuture<Layout> request1 = new CompletableFuture<>();
        CompletableFuture<Layout> request2 = new CompletableFuture<>();
        CompletableFuture<Layout> request3 = new CompletableFuture<>();
        request1.complete(buildSimpleLayout(2));
        request2.completeExceptionally(new TimeoutException("Connection error"));
        request3.completeExceptionally(new TimeoutException("Connection error"));

        HashMap<String, CompletableFuture<Layout>> requests = new HashMap<>();
        requests.put(NodeNames.A, request1);
        requests.put(NodeNames.B, request2);
        requests.put(NodeNames.C, request3);

        CompletableFuture<Boolean> asyncCommit = CompletableFuture.completedFuture(true);
        doReturn(asyncCommit).when(handlerSpy).commitLayout(isA(Layout.class), anyString());

        Layout layout1 = buildSimpleLayout();
        handlerSpy.updateTrailingLayoutServers(layout1, requests).join();

        verify(handlerSpy, times(3)).commitLayout(any(Layout.class), anyString());
    }

    @Test
    void commitLayout() {
        final boolean commitResult = true;
        EpochHandler handlerSpy = Mockito.spy(buildHandler());

        CompletableFuture<Boolean> asyncCommit = CompletableFuture.completedFuture(commitResult);
        doReturn(asyncCommit).when(handlerSpy).commitLayoutAsync(isA(Layout.class), anyString());

        Layout layout1 = buildSimpleLayout();
        Boolean result = handlerSpy.commitLayout(layout1, NodeNames.A).join();

        verify(handlerSpy, times(1)).commitLayoutAsync(any(Layout.class), anyString());

        assertEquals(commitResult, result);
    }

    @Test
    void commitLayoutConnectionError() {
        final boolean commitResult = true;
        EpochHandler handlerSpy = Mockito.spy(buildHandler());

        CompletableFuture<Boolean> asyncCommit = new CompletableFuture<>();
        asyncCommit.completeExceptionally(new TimeoutException("timeout exception"));
        doReturn(asyncCommit).when(handlerSpy).commitLayoutAsync(isA(Layout.class), anyString());

        Layout layout1 = buildSimpleLayout();
        Boolean result = handlerSpy.commitLayout(layout1, NodeNames.A).join();

        verify(handlerSpy, times(1)).commitLayoutAsync(any(Layout.class), anyString());

        assertFalse(result);
    }

    private EpochHandler buildHandler() {
        final int threads = 5;
        ExecutorService fdWorker = Executors.newFixedThreadPool(threads);
        return EpochHandler.builder()
                .corfuRuntime(Mockito.mock(CorfuRuntime.class))
                .serverContext(Mockito.mock(ServerContext.class))
                .failureDetectorWorker(fdWorker)
                .build();
    }
}

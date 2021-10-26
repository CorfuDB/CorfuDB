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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

        HashMap<String, CompletableFuture<Layout>> futureLayouts = new HashMap<>();
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

    private EpochHandler buildHandler() {
        return EpochHandler.builder()
                .corfuRuntime(Mockito.mock(CorfuRuntime.class))
                .serverContext(Mockito.mock(ServerContext.class))
                .build();
    }
}
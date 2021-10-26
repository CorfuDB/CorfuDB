package org.corfudb.infrastructure.management.failuredetector;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.view.Layout;

import java.util.concurrent.CompletableFuture;

@Slf4j
@AllArgsConstructor
public class FailureDetectorHelper {

    private final Layout layout;
    private final String localEndpoint;

    /**
     * Checks if this management client is allowed to handle reconfigurations.
     * - This client is not authorized to trigger reconfigurations if this node is not a part
     * of the current layout.
     *
     * @return True if node is allowed to handle reconfigurations. False otherwise.
     */
    public boolean canHandleReconfigurations() {

        // We check for the following condition here: If the node is NOT a part of the
        // current layout, it should not attempt to change layout.
        if (!layout.getAllServers().contains(localEndpoint)) {
            log.debug("This Server is not in the active layout. Aborting.");
            return false;
        }
        return true;
    }

    public CompletableFuture<Layout> handleReconfigurationsAsync() {
        if (canHandleReconfigurations()) {
            return CompletableFuture.completedFuture(layout);
        }

        String err = String.format(
                "Can't run failure detector. This Server: %s, doesn't belong to the active layout: %s",
                localEndpoint, layout
        );
        CompletableFuture<Layout> cf = new CompletableFuture<>();
        cf.completeExceptionally(new IllegalStateException(err));
        return cf;
    }
}

package org.corfudb.infrastructure.log.statetransfer.metrics;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.JsonUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Information about the recent state transfer.
 * It can be saved in the current node's data store.
 */
@ToString
public class StateTransferStats {

    public enum TransferMethod {
        PROTOCOL
    }

    @ToString
    @Builder
    public static class StateTransferAttemptStats {
        @NonNull
        private final String localEndpoint;
        @NonNull
        private final List<StateTransferManager.TransferSegment> initTransferSegments;
        @NonNull
        private final Layout layoutBeforeTransfer;
        @NonNull
        private final Duration durationOfTransfer;

        private final boolean succeeded;
        @Default
        private final TransferMethod method = TransferMethod.PROTOCOL;
        @Default
        private final Optional<Duration> durationOfRestoration = Optional.empty();
        @Default
        private final Optional<Layout> layoutAfterTransfer = Optional.empty();
    }

    private final List<StateTransferAttemptStats> attemptStats = new ArrayList<>();

    public void pushAttemptStats(StateTransferAttemptStats stats) {
        attemptStats.add(stats);
    }

    public String toJson() {
        return JsonUtils.toJson(this);
    }
}

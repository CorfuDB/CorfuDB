package org.corfudb.infrastructure.log.statetransfer.metrics;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.JsonUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

/**
 * Information about the recent state transfer.
 * It can be saved in the current node's data store.
 */
@ToString
@Slf4j
@AllArgsConstructor
public class StateTransferStats {

    public enum TransferMethod {
        PROTOCOL
    }

    @ToString
    @Builder
    @EqualsAndHashCode
    public static class StateTransferAttemptStats {
        @NonNull
        private final String localEndpoint;
        @NonNull
        private final List<StateTransferManager.TransferSegment> initTransferSegments;
        @NonNull
        private final Layout layoutBeforeTransfer;
        @NonNull
        private final Duration durationOfTransfer;
        @Default
        private final boolean succeeded = false;
        @Default
        private final TransferMethod method = TransferMethod.PROTOCOL;
        @Default
        private final Optional<Duration> durationOfRestoration = Optional.empty();
        @Default
        private final Optional<Layout> layoutAfterTransfer = Optional.empty();
    }

    @Getter
    private final ImmutableList<StateTransferAttemptStats> attemptStats;

    public String toJson() {
        return JsonUtils.toJson(this);
    }
}

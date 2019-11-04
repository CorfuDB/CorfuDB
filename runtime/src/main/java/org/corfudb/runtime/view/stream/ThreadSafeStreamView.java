package org.corfudb.runtime.view.stream;

import com.google.common.annotations.VisibleForTesting;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.StreamOptions;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

/**
 * A thread-safe implementation of IStreamView.
 *
 * Created by Maithem on 11/30/18.
 */

public class ThreadSafeStreamView implements IStreamView {

    private final IStreamView stream;

    public ThreadSafeStreamView(final CorfuRuntime runtime,
                                 final UUID streamId,
                                 @Nonnull final StreamOptions options) {
        if (runtime.getParameters().isFollowBackpointersEnabled()) {
            stream = new BackpointerStreamView(runtime, streamId, options);
        } else {
            stream = new AddressMapStreamView(runtime, streamId, options);
        }
    }

    public ThreadSafeStreamView(final CorfuRuntime runtime,
                                 final UUID streamId) {
        if (runtime.getParameters().isFollowBackpointersEnabled()) {
            stream = new BackpointerStreamView(runtime, streamId, StreamOptions.DEFAULT);
        } else {
            stream = new AddressMapStreamView(runtime, streamId, StreamOptions.DEFAULT);
        }
    }

    @Override
    public UUID getId() {
        return stream.getId();
    }

    @Override
    public synchronized void reset() {
        stream.reset();
    }

    @Override
    public synchronized void seek(long globalAddress) {
        stream.seek(globalAddress);
    }

    @Override
    public synchronized long find(long globalAddress, SearchDirection direction) {
        return stream.find(globalAddress, direction);
    }

    @Override
    public synchronized long append(Object object,
                Function<TokenResponse, Boolean> acquisitionCallback,
                Function<TokenResponse, Boolean> deacquisitionCallback) {
        return stream.append(object, acquisitionCallback, deacquisitionCallback);
    }

    @Override
    public synchronized ILogData next() {
        return stream.next();
    }

    @Override
    public synchronized ILogData previous() {
        return stream.previous();
    }

    @Override
    public synchronized ILogData current() {
        return stream.current();
    }

    @Override
    public synchronized ILogData nextUpTo(long maxGlobal) {
        return stream.nextUpTo(maxGlobal);
    }

    @Override
    public synchronized List<ILogData> remainingUpTo(long maxGlobal) {
        return stream.remainingUpTo(maxGlobal);
    }

    @Override
    public synchronized boolean hasNext() {
        return stream.hasNext();
    }

    @Override
    public synchronized long getCurrentGlobalPosition() {
        return stream.getCurrentGlobalPosition();
    }

    @Override
    public synchronized long getTotalUpdates() {
        return stream.getTotalUpdates();
    }

    @Override
    public synchronized long getCompactionMark() {
        return stream.getCompactionMark();
    }

    @VisibleForTesting
    IStreamView getUnderlyingStream() {
        return stream;
    }
}

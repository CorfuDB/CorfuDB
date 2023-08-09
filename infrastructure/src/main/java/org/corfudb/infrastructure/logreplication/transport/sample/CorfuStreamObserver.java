package org.corfudb.infrastructure.logreplication.transport.sample;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * GRPC streamObservers are not thread safe. This wrapper synchronizes the callbacks of streamObserver ensuring only a
 * single thread access.
 *
 * Its useful for scenarios where there are multiple threads trying to send messages on the same stream.
 */
public class CorfuStreamObserver<T> implements StreamObserver<T> {

    private final StreamObserver<T> observer;

    public CorfuStreamObserver(StreamObserver<T> observer) {
        this.observer = observer;
    }

    /**
     * Receives a value from the streams.
     *
     * @param value the value passed to the stream
     */
    @Override
    public synchronized void onNext(T value) {
        this.observer.onNext(value);
    }

    /**
     * Receives a terminating error from the stream.
     * @param t the error occurred on the stream
     */
    @Override
    public synchronized void onError(Throwable t) {
        this.observer.onError(t);
    }

    /**
     * Receives a notification of successful stream completion.
     */
    @Override
    public synchronized void onCompleted() {
        this.observer.onCompleted();
    }
}

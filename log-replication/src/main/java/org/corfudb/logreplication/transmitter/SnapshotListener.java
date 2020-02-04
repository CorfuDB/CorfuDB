package org.corfudb.logreplication.transmitter;

import java.util.List;

public interface SnapshotListener {

    boolean onNext(TxMessage message);

    boolean onNext(List<TxMessage> messages);

    /**
     * Call to the application on sync complete.
     */
    void complete();

    // Define error codes to pass
    void onError();
}

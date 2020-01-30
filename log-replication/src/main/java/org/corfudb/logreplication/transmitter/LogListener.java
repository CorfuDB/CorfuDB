package org.corfudb.logreplication.transmitter;

import java.util.List;

public interface LogListener {

    boolean onNext(TxMessage message);

    boolean onNext(List<TxMessage> messages);

    // Define error codes to pass
    void onError();
}

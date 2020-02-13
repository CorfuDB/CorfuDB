package org.corfudb.logreplication;

import org.corfudb.logreplication.message.DataMessage;

import java.util.List;

/**
 *
 *
 */
public interface DataReceiver {

    void receive(DataMessage message);

    void receive(List<DataMessage> messages);
}
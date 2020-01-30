package org.corfudb.logreplication.transmitter;

import org.corfudb.logreplication.MessageMetadata;

public class TxMessage {

    private MessageMetadata metadata;

    private byte[] data;
}

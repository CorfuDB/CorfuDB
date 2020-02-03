package org.corfudb.logreplication.transmitter;

import lombok.Data;
import org.corfudb.logreplication.MessageMetadata;

@Data
public class TxMessage {

    private MessageMetadata metadata;

    private byte[] data;


}

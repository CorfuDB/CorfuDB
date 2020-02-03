package org.corfudb.logreplication.receiver;

import lombok.Builder;
import lombok.Data;
import org.corfudb.logreplication.MessageMetadata;

@Builder
@Data
public class RxMessage {

    private MessageMetadata metadata;

    private byte[] data;
    
}

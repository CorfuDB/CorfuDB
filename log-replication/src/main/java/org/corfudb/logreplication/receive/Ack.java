package org.corfudb.logreplication.receive;

import lombok.Builder;
import lombok.Data;
import org.corfudb.logreplication.message.MessageMetadata;

@Builder
@Data
/**
 * Receiver acknowledgement
 */
public class Ack {

    MessageMetadata metadata;
}

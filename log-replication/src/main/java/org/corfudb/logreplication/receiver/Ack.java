package org.corfudb.logreplication.receiver;

import lombok.Builder;
import lombok.Data;
import org.corfudb.logreplication.MessageMetadata;

@Builder
@Data
/**
 * Receiver acknowledgement
 */
public class Ack {

    MessageMetadata metadata;
}

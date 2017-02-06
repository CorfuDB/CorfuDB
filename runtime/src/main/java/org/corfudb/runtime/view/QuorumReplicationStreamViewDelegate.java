/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.StreamCOWEntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.OverwriteException;

import java.util.*;

/**
 * Helper object for StreamView with the methods specific for quorum replication mode.
 * Created by Konstantin Spirov on 1/30/2017.
 */
@Slf4j
class QuorumReplicationStreamViewDelegate extends ChainReplicationStreamViewDelegate implements IStreamViewDelegate {
    private QuorumReplicationStreamViewDelegate() {
    }

    static class DelegateHolder {
        static final QuorumReplicationStreamViewDelegate delegate = new QuorumReplicationStreamViewDelegate();
    }
}

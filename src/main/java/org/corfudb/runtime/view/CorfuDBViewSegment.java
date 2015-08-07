/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.corfudb.runtime.view;

import org.corfudb.runtime.protocols.replications.IReplicationProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.corfudb.runtime.protocols.IServerProtocol;

import java.util.List;

/**
 * This class provides a view of the CorfuDB infrastructure. Clients
 * should not directly access the view without an interface.
 *
 * @author Michael Wei <mwei@cs.ucsd.edu>
 */

public class CorfuDBViewSegment {
    private static final Logger log = LoggerFactory.getLogger(CorfuDBViewSegment.class);

    private IReplicationProtocol replicationProtocol;
    private long start;
    private long sealed;

    public CorfuDBViewSegment(long start, long sealed, IReplicationProtocol replicationProtocol) {
        this.replicationProtocol = replicationProtocol;
        this.sealed = sealed;
        this.start = start;
    }

    public IReplicationProtocol getReplicationProtocol() {
        return replicationProtocol;
    }

    //TODO: Want to replace getGroups with this function, but need to figure out the semantics of getGroups first.
    public List<IServerProtocol> getLoggingUnits() { return replicationProtocol.getLoggingUnits(); }

    public List<List<IServerProtocol>> getGroups() {
        return replicationProtocol.getGroups(); }

    public long getStart() {
        return start;
    }

    public long getSealed() {
        return sealed;
    }
}


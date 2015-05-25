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
package org.corfudb.runtime.stream.legacy;


import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.HoleEncounteredException;
import org.corfudb.runtime.OutOfSpaceException;
import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.smr.IBundle;
import org.corfudb.runtime.smr.IStreamFactory;
import org.corfudb.runtime.stream.Bundle;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.Timestamp;
import org.corfudb.runtime.view.IStreamingSequencer;
import org.corfudb.runtime.view.IWriteOnceAddressSpace;
import org.corfudb.runtime.view.StreamingSequencer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutorService;
// import org.corfudb.client.view.ObjectCachedWriteOnceAddressSpace;


class HopAdapterStreamFactory implements IStreamFactory
{
    CorfuDBRuntime cdb;
    public HopAdapterStreamFactory(CorfuDBRuntime tcdb)
    {
        cdb = tcdb;
    }

    @Override
    public IStream newStream(UUID streamid)
    {
        return new HopAdapterStream(cdb, streamid);
    }
}

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

package org.corfudb.infrastructure;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.corfudb.infrastructure.thrift.StreamingSequencerService;
import org.corfudb.infrastructure.thrift.StreamSequence;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.runtime.view.IWriteOnceAddressSpace;
import org.corfudb.runtime.view.CachedWriteOnceAddressSpace;
import org.corfudb.runtime.entries.CorfuDBStreamMoveEntry;

import java.util.UUID;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.lang.Math;
import java.util.concurrent.CompletableFuture;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StreamingSequencerServer implements StreamingSequencerService.Iface, ICorfuDBServer {

    private int port = 0;
    private String configmasterURL = "";
    private Logger log = LoggerFactory.getLogger(StreamingSequencerServer.class);
    private CorfuDBClient c;
    private IWriteOnceAddressSpace woas;
    private ExecutorService tp = Executors.newCachedThreadPool();
    class StreamData {

        public StreamData(UUID streamID) {
            this.streamID = streamID;
            this.lastPos = new AtomicLong();
        }

        AtomicLong internalPos = null;
        AtomicLong lastPos = null;
        Long max = 0L;
        UUID streamID;
        int allocation = 10;

        synchronized void setAllocationSize(int size)
        {
            if (size < 2) { size = 2; }
            this.allocation = size;
        }

        synchronized void allocate()
        {
            if (internalPos == null)
            {
                Long newPos = pos.getAndAdd(allocation);
                internalPos = new AtomicLong(newPos);
                max = newPos + allocation - 1;
                log.debug("Stream  " + streamID + "allocation start at " + newPos + " to " + max);
            }
        }

        synchronized StreamSequence reallocate(StreamSequence s, int range)
        {
            if (s.position >= max)
            {
                Long newPos = pos.getAndAdd(allocation);
                internalPos = new AtomicLong(newPos);
                final long oldPos = s.position;
                CompletableFuture<Void> cf = CompletableFuture.runAsync(() -> {
                try {
                    CorfuDBStreamMoveEntry cdsme = new CorfuDBStreamMoveEntry(streamID, null, null, newPos, -1, -1);
                    log.debug("Writing move entry from " + oldPos + " to " + newPos + " for stream " + streamID);
                    woas.write(oldPos, cdsme);
                    lastPos.accumulateAndGet(oldPos, (cur, given) -> { return Math.max(cur,given);} );

                    log.debug("Finished writing move entry.");
                } catch (Exception ie)
                {
                    log.warn("Error placing move entry: ", ie);
                }
                }, tp);
                max = newPos + allocation - 1;
                s.position = internalPos.getAndAdd(range);
                s.totalTokens = Math.min(range, (int)(max-s.position));
                lastPos.accumulateAndGet(s.position, (cur, given) -> { return Math.max(cur,given);} );
                log.debug("Reallocation for " + streamID + " new range " + s.position + " - " + max);
            }
            else
            {
                s.position = internalPos.getAndAdd(range);
                lastPos.accumulateAndGet(s.position, (cur, given) -> { return Math.max(cur,given);} );
                s.totalTokens = Math.min(range, (int)(max-s.position));
            }

            return s;
        }

        StreamSequence getToken(int range)
        {
            StreamSequence s = new StreamSequence();
            if (range == 0) {s.position = lastPos.get(); return s;}
            if (internalPos == null) { allocate(); }
            s.position = internalPos.getAndAdd(range);
            s.totalTokens = Math.min(range, (int)(max-s.position));
            if (s.position >= max)
            {
                return reallocate(s, range);
            }
            lastPos.accumulateAndGet(s.position, (cur, given) -> { return Math.max(cur,given);} );
            return s;
        }
    }
	AtomicLong pos = new AtomicLong(0);
    Map<String, StreamData> streamMap = new ConcurrentHashMap<String, StreamData>();

    @Override
    public StreamSequence nextstreampos(String streamID, int range) throws TException {
        StreamData sd = streamMap.get(streamID);
        if (sd == null)
        {
            log.debug("Registering new stream " + streamID);
            StreamData sd_temp = new StreamData(UUID.fromString(streamID));
            sd = streamMap.putIfAbsent(streamID, sd_temp);
            if (sd == null) { sd = sd_temp; }
            sd.allocate();
        }
        return sd.getToken(range);
    }

    @Override
    public void setAllocationSize(String streamID, int size) throws TException {
        StreamData sd = streamMap.get(streamID);
        if (sd == null)
        {
            log.debug("Registering new stream " + streamID);
            StreamData sd_temp = new StreamData(UUID.fromString(streamID));
            sd = streamMap.putIfAbsent(streamID, sd_temp);
            if (sd == null) { sd = sd_temp; }
        }
        log.debug("Setting " + streamID + " allocation size to " + size);
        sd.setAllocationSize(size);
    }

    @Override
    public void reset() throws TException {
        log.info("Reset requested, resetting maps and counters...");
        streamMap = new ConcurrentHashMap<String, StreamData>();
        pos = new AtomicLong(0);
    }

	@Override
	public long nextpos(int range) throws TException {
		// if (pos % 10000 == 0) System.out.println("issue token " + pos + "...");
		long ret = pos.getAndAdd(range);
		return ret;
	}

    @Override
    public void recover(long lowbound) throws TException {
        pos.set(lowbound);
    }

    @Override
    public boolean ping() throws TException {
        return true;
    }

    public Runnable getInstance(final Map<String,Object> config)
    {
        final StreamingSequencerServer st = this;
        return new Runnable()
        {
            @Override
            public void run() {
                st.port = (Integer) config.get("port");
                st.configmasterURL = (String) config.get("configmaster");
                st.c = new CorfuDBClient(st.configmasterURL);
                st.c.startViewManager();
                st.woas = new CachedWriteOnceAddressSpace(st.c);
                while (true) {
                    st.serverloop();
                }
            }
        };
    }

	public void serverloop() {

        TServer server;
        TServerSocket serverTransport;
        StreamingSequencerService.Processor<StreamingSequencerServer> processor;
        log.debug("Streaming sequencer entering service loop.");
        try {
            serverTransport = new TServerSocket(port);
            processor =
                    new StreamingSequencerService.Processor<StreamingSequencerServer>(this);
            server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
            log.info("Streaming sequencer starting on port " + port);
            server.serve();
        } catch (TTransportException e) {
            log.warn("Streaming sequencer encountered exception, restarting.", e);
        }
	}
}

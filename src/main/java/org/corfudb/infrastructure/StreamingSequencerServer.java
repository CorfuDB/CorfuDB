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

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Getter;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import org.corfudb.runtime.CorfuDBRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.corfudb.infrastructure.thrift.StreamingSequencerService;
import org.corfudb.infrastructure.thrift.StreamSequence;

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
    private Logger log = LoggerFactory.getLogger(StreamingSequencerServer.class);
    private boolean simFailure = false;

    @Getter
    Thread thread;
    TServer server;
    boolean running;

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        running = true;
        while (running)
        {
            serverloop();
        }
    }

	AtomicLong pos = new AtomicLong(0);

    @Override
    public StreamSequence nextstreampos(String streamID, int range) throws TException {
        if (simFailure)
        {
            throw new TException("Simulated failure mode!");
        }
        StreamSequence ss = new StreamSequence();
        ss.setPosition(nextpos(range));
        ss.setTotalTokens(1);
        return ss;
    }

    @Override
    public void simulateFailure(boolean fail, long length)
            throws TException
    {
        if (fail && length != -1)
        {
            this.simFailure = true;
            final StreamingSequencerServer t = this;
            new Timer().schedule(
                    new TimerTask() {
                        @Override
                        public void run() {
                            t.simFailure = false;
                        }
                    },
                    length
            );
        }
        else {
            this.simFailure = fail;
        }
    }
    @Override
    public void setAllocationSize(String streamID, int size) throws TException {
        if (simFailure)
        {
            throw new TException("Simulated failure mode!");
        }
        throw new TException("deprecated");
    }

    @Override
    public void reset() throws TException {
        log.info("Reset requested, resetting maps and counters...");
        pos = new AtomicLong(0);
        simFailure = false;
    }

	@Override
	public long nextpos(int range) throws TException {
        if (simFailure)
        {
            throw new TException("Simulated failure mode!");
        }
		// if (pos % 10000 == 0) System.out.println("issue token " + pos + "...");
		long ret = pos.getAndAdd(range);
		return ret;
	}

    @Override
    public void recover(long lowbound) throws TException {
        if (simFailure)
        {
            throw new TException("Simulated failure mode!");
        }
        pos.set(lowbound);
    }

    @Override
    public boolean ping() throws TException {
        if (simFailure)
        {
            throw new TException("Simulated failure mode!");
        }
        return true;
    }

    public ICorfuDBServer getInstance(final Map<String,Object> config)
    {
        final StreamingSequencerServer st = this;
        st.port = (Integer) config.get("port");
        thread = new Thread(this);
        return this;
    }

    @Override
    public void close() {
        running = false;
        server.stop();
        thread.interrupt();
        try {
            this.getThread().join(5000);
        } catch(InterruptedException ie)
        {

        }
    }

    public void serverloop() {
        TServerSocket serverTransport;
        StreamingSequencerService.Processor<StreamingSequencerServer> processor;
        log.debug("Streaming sequencer entering service loop.");
        try {
            serverTransport = new TServerSocket(port);
            processor =
                    new StreamingSequencerService.Processor<StreamingSequencerServer>(this);
            server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport)
                    .processor(processor)
                    .protocolFactory(TCompactProtocol::new)
                    .inputTransportFactory(new TFastFramedTransport.Factory())
                    .outputTransportFactory(new TFastFramedTransport.Factory())
            );
            log.info("Streaming sequencer starting on port " + port);
            server.serve();
        } catch (TTransportException e) {
            log.warn("Streaming sequencer encountered exception, restarting.", e);
        }
	}
}

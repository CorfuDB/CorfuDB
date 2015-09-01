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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.corfudb.infrastructure.thrift.SimpleSequencerService;

import java.util.Map;

public class SimpleSequencerServer implements SimpleSequencerService.Iface, ICorfuDBServer {

    private int port = 0;
    private Logger log = LoggerFactory.getLogger(SimpleSequencerServer.class);
    @Getter
    Thread thread;
    boolean running;
    TServer server;
	AtomicLong pos = new AtomicLong(0);
    boolean simFailure;

	@Override
	public long nextpos(int range) throws TException {
		// if (pos % 10000 == 0) System.out.println("issue token " + pos + "...");
        if (simFailure)
        {
            throw new TException("Simulated failure mode!");
        }
		long ret = pos.getAndAdd(range);
		return ret;
	}

    @Override
    public void reset() throws TException {
        log.info("Reset requested, setting sequence to 0");
        simFailure = false;
        pos = new AtomicLong(0);
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
        final SimpleSequencerServer st = this;
        st.port = (Integer) config.get("port");
        thread = new Thread(this);
        return this;
    }

    @Override
    public void close() {
        running = false;
        server.stop();
    }

    @Override
    public void simulateFailure(boolean fail, long length)
            throws TException
    {
        if (fail && length != -1)
        {
            this.simFailure = true;
            final SimpleSequencerServer t = this;
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

	public void serverloop() {
        TServerSocket serverTransport;
        SimpleSequencerService.Processor<SimpleSequencerServer> processor;
        log.debug("Simple sequencer entering service loop.");
        try {
            serverTransport = new TServerSocket(port);
            processor =
                    new SimpleSequencerService.Processor<SimpleSequencerServer>(this);
            server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport)
                    .processor(processor)
                    .protocolFactory(TCompactProtocol::new)
                    .inputTransportFactory(new TFastFramedTransport.Factory())
                    .outputTransportFactory(new TFastFramedTransport.Factory())
            );
            log.info("Simple sequencer starting on port " + port);
            server.serve();
        } catch (TTransportException e) {
            log.warn("SimpleSequencer encountered exception, restarting.", e);
        }
	}

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
            this.serverloop();
        }
    }
}

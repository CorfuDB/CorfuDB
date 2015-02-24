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

import org.corfudb.infrastructure.thrift.SimpleSequencerService;

import java.util.Map;

public class SimpleSequencerServer implements SimpleSequencerService.Iface, ICorfuDBServer {

    private int port = 0;
    private Logger log = LoggerFactory.getLogger(SimpleSequencerServer.class);

	AtomicLong pos = new AtomicLong(0);

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
        final SimpleSequencerServer st = this;
        return new Runnable()
        {
            @Override
            public void run() {
                st.port = (Integer) config.get("port");
                while (true) {
                    st.serverloop();
                }
            }
        };
    }

	public void serverloop() {

        TServer server;
        TServerSocket serverTransport;
        SimpleSequencerService.Processor<SimpleSequencerServer> processor;
        log.debug("Simple sequencer entering service loop.");
        try {
            serverTransport = new TServerSocket(port);
            processor =
                    new SimpleSequencerService.Processor<SimpleSequencerServer>(this);
            server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
            log.info("Simple sequencer starting on port " + port);
            server.serve();
        } catch (TTransportException e) {
            log.warn("SimpleSequencer encountered exception, restarting.", e);
        }
	}
}

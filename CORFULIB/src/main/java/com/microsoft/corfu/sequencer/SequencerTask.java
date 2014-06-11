package com.microsoft.corfu.sequencer;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

public class SequencerTask implements CorfuSequencer.Iface {

    public static int port = 0;

	AtomicLong pos = new AtomicLong(0);
	
	@Override
	public long nextpos(int range) throws org.apache.thrift.TException {
		// if (pos % 10000 == 0) System.out.println("issue token " + pos + "...");
		long ret = pos.getAndAdd(range);
		return ret;
	}

    @Override
    public void recover(long lowbound) throws TException {
        pos.set(lowbound);
    }

	public void serverloop() {
			
        TServer server;
        TServerSocket serverTransport;
        CorfuSequencer.Processor<SequencerTask> processor;
        System.out.println("run..");

        try {
            serverTransport = new TServerSocket(port);
            processor =
                    new CorfuSequencer.Processor<SequencerTask>(this);
            server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
            System.out.println("Starting sequencer on port " + port);

            server.serve();
        } catch (TTransportException e) {
            e.printStackTrace();
        }
	}
}

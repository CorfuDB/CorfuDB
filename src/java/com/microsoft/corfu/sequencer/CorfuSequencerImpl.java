package com.microsoft.corfu.sequencer;

import java.io.File;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.microsoft.corfu.CorfuConfigManager;
import com.microsoft.corfu.sequencer.CorfuSequencer;
import com.microsoft.corfu.sequencer.CorfuSequencer.Iface;
import com.microsoft.corfu.sequencer.CorfuSequencer.Processor;

public class CorfuSequencerImpl implements CorfuSequencer.Iface {
	
	long pos = 0;
	
	public long nextpos() throws org.apache.thrift.TException {
		// if (pos % 10000 == 0) System.out.println("issue token " + pos + "...");
		return pos++;
	}
	
	static class dorun implements Runnable {
		
		CorfuSequencerImpl CI;
		int port = 0;

		public dorun(int port, CorfuSequencerImpl CI) {
			super();
			this.port = port;
			this.CI = CI;
		}
		
		@Override
		public void run() {
			
			TServer server;
			TServerSocket serverTransport;
			CorfuSequencer.Processor<CorfuSequencerImpl> processor; 
			System.out.println("run..");
	
			try {
				serverTransport = new TServerSocket(port);
				processor = 
						new CorfuSequencer.Processor<CorfuSequencerImpl>(CI);
				server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
				System.out.println("Starting sequencer on port " + port);
				
				server.serve();
			} catch (TTransportException e) {
				e.printStackTrace();
			}
		}
	}
	
	static class dostats implements Runnable {
		
		CorfuSequencerImpl CI;
		
		public dostats(CorfuSequencerImpl CI) {
			super();
			this.CI = CI;
		}
		
		@Override
		public void run() {
			long starttime = System.currentTimeMillis();
			long elapsetime = 0;
			long lastpos = -1;

			while (true) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (lastpos != CI.pos) {
					elapsetime = System.currentTimeMillis() - starttime;
					System.out.println("++stats: pos=" + CI.pos/1000 + "K elapse ~" + elapsetime/1000 + " seconds");
					lastpos = CI.pos;
				}
			}
		}
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		CorfuConfigManager CM = new CorfuConfigManager(new File("./0.aux"));

		int port = CM.getSequencer().getPort();
		CorfuSequencerImpl CI = new CorfuSequencerImpl();
		new Thread(new dorun(port, CI)).start();
		new Thread(new dostats(CI)).start();
	}

}

package com.microsoft.corfu.sa;

import org.apache.thrift.transport.*;
import org.apache.thrift.server.*;

import com.microsoft.corfu.CorfuConfigManager;
import com.microsoft.corfu.CorfuNode;

import java.io.File;
import java.util.List;
import java.util.ArrayList;

public class CorfuStandaloneServerWrapper implements Runnable {
	
	int port;

	public CorfuStandaloneServerWrapper(int port) {
		this.port = port;
	}

	@Override
	public void run() {
		
		TServer server;
		TServerSocket serverTransport;
		CorfuStandaloneServer.Processor<CorfuStandaloneServerImpl> processor; 
		System.out.println("run..");

		try {
			serverTransport = new TServerSocket(port);
			processor = 
					new CorfuStandaloneServer.Processor<CorfuStandaloneServerImpl>(new CorfuStandaloneServerImpl());
			server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
			System.out.println("Starting server on port " + port);
			
			server.serve();
		} catch (TTransportException e) {
			e.printStackTrace();
		}
	}
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		CorfuConfigManager CM = new CorfuConfigManager(new File("./0.aux"));

		// kludge; assume single storage unit for now!!
		CorfuNode sunit = CM.getGroupByNumber(0)[0];
		new Thread(new CorfuStandaloneServerWrapper(sunit.getPort())).run();
	}
}


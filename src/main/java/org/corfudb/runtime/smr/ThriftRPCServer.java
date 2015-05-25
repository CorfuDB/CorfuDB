package org.corfudb.runtime.smr;

import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.corfudb.runtime.smr.legacy.IRPCServer;
import org.corfudb.runtime.smr.legacy.IRPCServerHandler;

/**
 * Created by crossbach on 5/22/15.
 */

public class ThriftRPCServer implements IRPCServer {

    public ThriftRPCServer() {
    }

    public void registerHandler(int portnum, IRPCServerHandler h) {
        final IRPCServerHandler handler = h;
        final TServer server;
        TServerSocket serverTransport;
        //RemoteReadService.Processor<RemoteReadService.Iface> processor;
        try {
            serverTransport = new TServerSocket(portnum);
            // processor = new RemoteReadService.Processor(new RemoteReadService.Iface()
            //  {
            //    @Override
            //     public ByteBuffer remote_read(ByteBuffer arg) throws TException
            //      {
            //         return Utils.serialize(handler.deliverRPC(Utils.deserialize(arg)));
            //      }
            //  });
            // server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
            new Thread(new Runnable() {
                @Override
                public void run() {
                    //  server.serve(); //this seems to be a blocking call, putting it in its own thread
                }
            }).start();
            System.out.println("listening on port " + portnum);
        } catch (TTransportException e) {
            throw new RuntimeException(e);
        }
    }

}

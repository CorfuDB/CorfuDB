package org.corfudb.runtime.smr;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.corfudb.runtime.smr.legacy.IRPCClient;

import java.io.Serializable;

/**
 * Created by crossbach on 5/22/15.
 */
public class ThriftRPCClient implements IRPCClient
{
    public void ThriftRPCClient()
    {

    }
    public Object send(Serializable command, String hostname, int portnum)
    {
        try
        {
            //todo: make this less brain-dead
            TTransport transport = new TSocket(hostname, portnum);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            //  RemoteReadService.Client client = new RemoteReadService.Client(protocol);

            //    Object ret = Utils.deserialize(client.remote_read(Utils.serialize(command)));
            // transport.close();
            return null;
        }
        catch (TTransportException e)
        {
            throw new RuntimeException(e);
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
    }
}

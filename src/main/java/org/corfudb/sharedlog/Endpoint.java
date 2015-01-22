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
package org.corfudb.sharedlog;

import org.corfudb.sharedlog.loggingunit.LogUnitConfigService;
import org.corfudb.sharedlog.loggingunit.LogUnitService;
import org.corfudb.sharedlog.sequencer.SequencerService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.HashMap;

// This is a Corfu endpoint
//
public class Endpoint {
	static private HashMap<String, Endpoint> epmap = new HashMap<String, Endpoint>();

    private String hostname;
	private int port;
    private Object info;

    // constructor is private; use genEndpoint to generate a new endpoint!
	Endpoint(String fullname)
	{
		hostname = fullname.substring(0, fullname.indexOf(":"));
		port = Integer.parseInt(fullname.substring(fullname.indexOf(":")+1));
        info = null;
	}

    public static Endpoint genEndpoint(String fullname) {
        if (epmap.containsKey(fullname))
            return epmap.get(fullname);
        else {
            Endpoint ep = new Endpoint(fullname);
            if (ep.getPort() <0) return null;
            epmap.put(fullname, ep);
            return ep;
        }
    }

    static class clientSunitEndpoint {
        TTransport t = null;
        LogUnitService.Client cl = null;
        TBinaryProtocol protocol = null;
        LogUnitConfigService.Client configcl = null;

        clientSunitEndpoint(Endpoint cn) throws CorfuException {
            TMultiplexedProtocol mprotocol = null, mprotocol2 = null;

            try {
                t = new TSocket(cn.getHostname(), cn.getPort());
                t.open();
                protocol = new TBinaryProtocol(t);

                mprotocol = new TMultiplexedProtocol(protocol, "SUNIT");
                cl = new LogUnitService.Client(mprotocol);

                mprotocol2 = new TMultiplexedProtocol(protocol, "CONFIG");
                configcl = new LogUnitConfigService.Client(mprotocol2);

            } catch (TTransportException e) {
                e.printStackTrace();
                throw new CorfuException("could not set up connection(s)");
            }
        }
    }

    static public LogUnitService.Client getSUnitOf(Endpoint cn) throws CorfuException {
        clientSunitEndpoint ep = (clientSunitEndpoint) cn.getInfo();
        if (ep == null) {
            ep = new clientSunitEndpoint(cn);
            cn.setInfo(ep);
        }
        return ep.cl;
    }
    static public LogUnitConfigService.Client getCfgOf(Endpoint cn) throws CorfuException {
        clientSunitEndpoint ep = (clientSunitEndpoint) cn.getInfo();
        if (ep == null) {
            ep = new clientSunitEndpoint(cn);
            cn.setInfo(ep);
        }
        return ep.configcl;
    }

    static class clientSequencerEndpoint {
        TTransport t = null;
        SequencerService.Client cl = null;
        TBinaryProtocol protocol = null;

        clientSequencerEndpoint(Endpoint cn) throws CorfuException {
            try {
                t = new TSocket(cn.getHostname(), cn.getPort());
                protocol = new TBinaryProtocol(t);
                cl = new SequencerService.Client(protocol);
                t.open();
            } catch (TTransportException e) {
                e.printStackTrace();
                throw new CorfuException("could not set up connection(s)");
            }
        }
    }
    static public SequencerService.Client getSequencer(Endpoint cn) throws CorfuException {
        clientSequencerEndpoint ep = (clientSequencerEndpoint) cn.getInfo();
        if (ep == null) {
            ep = new clientSequencerEndpoint(cn);
            cn.setInfo(ep);
        }
        return ep.cl;
    }

    @Override
	public String toString()
	{
		return hostname + ":" + port;
	}

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Object getInfo() {
        return info;
    }

    public void setInfo(Object info) {
        this.info = info;
    }

}

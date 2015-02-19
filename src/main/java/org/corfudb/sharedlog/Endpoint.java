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

import org.corfudb.infrastructure.thrift.SimpleLogUnitConfigService;
import org.corfudb.infrastructure.thrift.SimpleLogUnitService;
import org.corfudb.infrastructure.thrift.SimpleSequencerService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.HashMap;
import java.util.Map;

// This is a Corfu endpoint
//
public class Endpoint {

    // caching endpoint objects blindly disables different ClientLib instances
    // from being able to have their own connection, which in turn exposes
    // thread safety problems in thrift. Solution: cache endpoints per thread
    static public boolean bCacheEndpoints = true;
    static private HashMap<Long, HashMap<String, Endpoint>> epmap = new HashMap<Long, HashMap<String, Endpoint>>();

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
        HashMap<String, Endpoint> threadCache = null;
        long tid = Thread.currentThread().getId();
        if (bCacheEndpoints) {
            synchronized (epmap){
                if(!epmap.containsKey(tid))
                    epmap.put(tid, new HashMap<String, Endpoint>());
                threadCache = epmap.get(tid);
                if(threadCache.containsKey(fullname))
                    return threadCache.get(fullname);
            }
        }
        Endpoint ep = new Endpoint(fullname);
        if (ep.getPort() <0) return null;
        if(bCacheEndpoints) {
            synchronized (epmap) {
                assert(threadCache != null);
                threadCache.put(fullname, ep);
            }
        }
        return ep;
    }

    protected static interface mpclientctor {
        public Object create(TMultiplexedProtocol mprotocol);
    }

    protected static interface bpclientctor {
        public Object create(TBinaryProtocol protocol);
    }

    static class ThreadTransport {

        long m_tid = 0;
        boolean m_valid = false;
        Endpoint m_parent = null;
        TTransport m_t = null;
        TBinaryProtocol m_protocol = null;
        HashMap<String, Object> m_clients = new HashMap<String, Object>();

        private ThreadTransport(
                Endpoint cn,
                HashMap<String, bpclientctor> binaryClients,
                HashMap<String, mpclientctor> multiplexedClients) throws CorfuException {

            m_valid = false;
            m_parent = cn;
            initialize(Thread.currentThread().getId(), cn, binaryClients, multiplexedClients);
        }

        private void
        initialize(
                long tid,
                Endpoint cn,
                HashMap<String, bpclientctor> binaryProtocolClients,
                HashMap<String, mpclientctor> multiplexedProtocolClients) throws CorfuException {

            m_valid = false;
            m_parent = cn;
            m_tid = tid;

            try {

                // All endpoint objects require at minimum per-thread transport state,
                // which involves creating and opening a socket, and setting up a TBinaryProtocol
                // on top of that transport. Sequencer clients require only that binary protocol
                // transport and expose client objects built on top of those. Log service clients
                // use multiplexed protocols built on top of the binary protocol, and only explose
                // client objects based on the multi-plexed protocol. Start by dealing with the
                // socket and binary protocol that every one needs.
                m_t = new TSocket(m_parent.getHostname(), cn.getPort());
                m_t.open();
                m_protocol = new TBinaryProtocol(m_t);

                if(binaryProtocolClients != null) {

                    // if we have a map of binary protocol client names and ctors for creating them,
                    // it means the sub-class is planning to expose clients created
                    // from the binary protocol. create the clients, add them to the map.

                    for(Map.Entry<String, bpclientctor> entry : binaryProtocolClients.entrySet()) {
                        String sname = entry.getKey();
                        bpclientctor ctor = entry.getValue();
                        m_clients.put(sname, ctor.create(m_protocol));
                    }
                }

                if(multiplexedProtocolClients != null) {

                    // if we have a map of multiplexed protocol client names and ctors for creating them,
                    // it means the sub-class is planning to expose clients created
                    // from individual mulitplexed protocols. create the multiplexed protocols,
                    // then the clients, and add them to the map.

                    for(Map.Entry<String, mpclientctor> entry : multiplexedProtocolClients.entrySet()) {
                        String sname = entry.getKey();
                        mpclientctor ctor = entry.getValue();
                        TMultiplexedProtocol mp = new TMultiplexedProtocol(m_protocol, sname);
                        m_clients.put(sname, ctor.create(mp));
                    }
                }

                // if we got to here without taking an exception, then we've successfully
                // instantiated the per-thread transport and protocol objects we need.
                m_valid = true;

            } catch (TTransportException e) {
                e.printStackTrace();
                throw new CorfuException("could not set up connection(s)");
            }
        }

        Object client(String sname) {
            assert(m_tid == Thread.currentThread().getId());
            return m_clients.get(sname);
        }
    }

    static class ClientEndpoint {

        Endpoint m_cn = null;
        HashMap<Long, ThreadTransport> m_tmap = new HashMap<Long, ThreadTransport>();
        HashMap<String, bpclientctor> m_bclients;
        HashMap<String, mpclientctor> m_mclients;

        ClientEndpoint(
                Endpoint cn,
                HashMap<String, bpclientctor> binaryClients,
                HashMap<String, mpclientctor> multiplexedClients) throws CorfuException {
            m_cn = cn;
            m_bclients = binaryClients;
            m_mclients = multiplexedClients;
            m_tmap.put(Thread.currentThread().getId(), new ThreadTransport(m_cn, m_bclients, m_mclients));
        }

        ClientEndpoint(
                Endpoint cn,
                Object... vclients) throws CorfuException {

            m_cn = cn;
            m_bclients = new HashMap<String, bpclientctor>();
            m_mclients = new HashMap<String, mpclientctor>();

            // convention is vclients is a flattened list of pairs of
            // the form <String, clientctor>, where clientctor is a lambda that
            // that takes an protocol object and returns a corfu client interface
            // consequently, there are some obvious invariants to check.
            assert(vclients.length % 2 == 0);
            for(int i=0; i<vclients.length; i+=2) {
                Object oname = vclients[i];
                Object octor = vclients[i+1];
                assert(oname instanceof String);
                assert(octor instanceof mpclientctor || octor instanceof bpclientctor);
                if(octor instanceof mpclientctor)
                    m_mclients.put((String) oname, (mpclientctor) octor);
                else
                    m_bclients.put((String) oname, (bpclientctor) octor);
            }
            m_tmap.put(Thread.currentThread().getId(), new ThreadTransport(m_cn, m_bclients, m_mclients));
        }

        protected ThreadTransport getTransport() throws CorfuException {
            ThreadTransport tstate = null;
            long tid = Thread.currentThread().getId();
            synchronized (m_tmap) {
                if (!m_tmap.containsKey(tid)) {
                    m_tmap.put(tid, new ThreadTransport(m_cn, m_bclients, m_mclients));
                }
                tstate = m_tmap.get(tid);
            }
            return tstate;
        }

        protected Object client(String sname) throws CorfuException {
            assert (m_cn != null);
            ThreadTransport tstate = getTransport();
            return tstate.client(sname);
        }
    }

    static class clientSunitEndpoint extends ClientEndpoint {

        static mpclientctor s_luclientctor = (a) -> new SimpleLogUnitService.Client(a);
        static mpclientctor s_luconfigctor = (a) -> new SimpleLogUnitConfigService.Client(a);

        clientSunitEndpoint(Endpoint cn) throws CorfuException {
            super(cn, "SUNIT", s_luclientctor, "CONFIG", s_luconfigctor);
        }

        SimpleLogUnitService.Client cl() throws CorfuException {
            ThreadTransport tstate = getTransport();
            return (SimpleLogUnitService.Client) tstate.client("SUNIT");
        }

        SimpleLogUnitConfigService.Client configcl() throws CorfuException {
            ThreadTransport tstate = getTransport();
            return (SimpleLogUnitConfigService.Client) tstate.client("CONFIG");
        }
    }

    static class clientSequencerEndpoint extends ClientEndpoint {

        static bpclientctor s_seqclientctor = (a) -> new SimpleSequencerService.Client(a);
        clientSequencerEndpoint(Endpoint cn) throws CorfuException {
            super(cn, "SEQ", s_seqclientctor);
        }
        SimpleSequencerService.Client cl() throws CorfuException {
            ThreadTransport tstate = getTransport();
            return (SimpleSequencerService.Client) tstate.client("SEQ");
        }
    }

    static public SimpleLogUnitService.Client getSUnitOf(Endpoint cn) throws CorfuException {
        clientSunitEndpoint ep = (clientSunitEndpoint) cn.getInfo();
        if (ep == null) {
            ep = new clientSunitEndpoint(cn);
            cn.setInfo(ep);
        }
        return ep.cl();
    }
    static public SimpleLogUnitConfigService.Client getCfgOf(Endpoint cn) throws CorfuException {
        clientSunitEndpoint ep = (clientSunitEndpoint) cn.getInfo();
        if (ep == null) {
            ep = new clientSunitEndpoint(cn);
            cn.setInfo(ep);
        }
        return ep.configcl();
    }

    static public SimpleSequencerService.Client getSequencer(Endpoint cn) throws CorfuException {
        clientSequencerEndpoint ep = (clientSequencerEndpoint) cn.getInfo();
        if (ep == null) {
            ep = new clientSequencerEndpoint(cn);
            cn.setInfo(ep);
        }
        return ep.cl();
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

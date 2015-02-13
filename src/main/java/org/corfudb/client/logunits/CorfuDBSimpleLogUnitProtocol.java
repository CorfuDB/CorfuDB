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

package org.corfudb.client.logunits;

import org.corfudb.client.IServerProtocol;
import org.corfudb.client.PooledThriftClient;
import org.corfudb.infrastructure.thrift.SimpleLogUnitService;

import org.apache.thrift.protocol.TProtocol;
import org.apache.commons.pool.impl.GenericObjectPool.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CorfuDBSimpleLogUnitProtocol implements IServerProtocol
{
    private String host;
    private String port;
    private String fullString;

    private final PooledThriftClient<SimpleLogUnitService.Client> thriftPool;
    private Logger log = LoggerFactory.getLogger(CorfuDBSimpleLogUnitProtocol.class);

     public static String getProtocolString()
    {
        return "cdbslu";
    }

    public static IServerProtocol protocolFactory(String host, String port, String fullString)
    {
        return new CorfuDBSimpleLogUnitProtocol(host, port, fullString);
    }

    public CorfuDBSimpleLogUnitProtocol(String host, String port, String fullString)
    {
        this.host = host;
        this.port = port;
        this.fullString = fullString;

        try
        {
            thriftPool = new PooledThriftClient<SimpleLogUnitService.Client>(
                    new PooledThriftClient.ClientFactory<SimpleLogUnitService.Client>() {
                        @Override
                        public SimpleLogUnitService.Client make(TProtocol protocol)
                        {
                            return new SimpleLogUnitService.Client(protocol);
                        }
                    },
                    new Config(),
                    host,
                    Integer.parseInt(port)
            );
        }
        catch (Exception ex)
        {
            log.warn("Failed to connect to endpoint " + fullString);
            throw new RuntimeException("Failed to connect to endpoint");
        }
    }

    public String getFullString()
    {
        return getProtocolString() + "://" + host + ":" + port;
    }

    public boolean ping()
    {
        SimpleLogUnitService.Client client = null;
        try {
            client = thriftPool.getResource();
            boolean ret = client.ping();
            thriftPool.returnResourceObject(client);
            return ret;
        }
        catch (Exception e)
        {
            if (client != null ) {thriftPool.returnBrokenResource(client);}
            return false;
        }

    }
}



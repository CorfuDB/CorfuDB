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

import org.corfudb.client.CorfuDBView;
import org.corfudb.sharedlog.ICorfuDBServer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.thrift.TException;

import org.slf4j.MarkerFactory;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.*;

import javax.json.JsonWriter;
import javax.json.Json;

public class ConfigMasterService implements Runnable, ICorfuDBServer {

    private Logger log = LoggerFactory.getLogger(ConfigMasterService.class);
    private Map<String,Object> config;
    private CorfuDBView currentView;

    int masterid = new SecureRandom().nextInt();

    public ConfigMasterService() {
    }

    public Runnable getInstance(final Map<String,Object> config)
    {
        //use the config for the init view (well, we'll have to deal with reconfigurations...)
        this.config = config;
        currentView = new CorfuDBView(config);
        return this;
    }

    public void run()
    {
        try {
            log.info("Starting HTTP Service on port " + config.get("port"));
            HttpServer server = HttpServer.create(new InetSocketAddress((Integer)config.get("port")), 0);
            server.createContext("/corfu", new RequestHandler());
            server.setExecutor(null);
            server.start();
        } catch(IOException ie) {
            log.error(MarkerFactory.getMarker("FATAL"), "Couldn't start HTTP Service!" , ie);
            System.exit(1);
        }
    }


    private class RequestHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {

            String response = null;

            if (t.getRequestMethod().startsWith("GET")) {
                log.debug("GET request");
                StringWriter sw = new StringWriter();
                try (JsonWriter jw = Json.createWriter(sw))
                {
                    jw.writeObject(currentView.getSerializedJSONView());
                }
                response = sw.toString();
                log.debug("Response is", response);
            } else {
                log.debug("PUT request");
                response = "deny";
            }
                t.sendResponseHeaders(200, response.length());
                OutputStream os = t.getResponseBody();
                os.write(response.getBytes());
                os.close();

        }
    }

}


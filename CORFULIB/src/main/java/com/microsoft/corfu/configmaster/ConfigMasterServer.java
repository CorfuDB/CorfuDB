package com.microsoft.corfu.configmaster;

import com.microsoft.corfu.CorfuConfiguration;
import com.microsoft.corfu.CorfuException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

/**
 * Created by dalia on 6/9/2014.
 */
public class ConfigMasterServer {

    static CorfuConfiguration C;

    public static void main(String[] args) throws IOException {
        C = new CorfuConfiguration(new File("./corfu.xml"));

        HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);
        server.createContext("/corfu", new MyHandler());
        server.setExecutor(null);
        server.start();
    }

    static class MyHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {

            String response = null;
            if (t.getRequestMethod().startsWith("GET")) {
                response = C.ConfToXMLString();
            }

            else {
                CorfuConfiguration NEWC = null;
                try {
                    NEWC = new CorfuConfiguration(t.getRequestBody());
                    if (NEWC.getEpoch() <= C.getEpoch()) {
                        response = "deny";
                    } else {
                        C = NEWC;
                        response = "approve";
                    }
                } catch (CorfuException e) {
                    e.printStackTrace();
                    response = "deny";
                }

            }
            t.sendResponseHeaders(200, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }

}

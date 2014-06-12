package com.microsoft.corfu.configmaster;

import com.microsoft.corfu.CorfuConfiguration;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.*;

/**
 * Created by dalia on 6/9/2014.
 */
public class ConfigClnt {
    public static void main(String[] args) throws IOException {

        DefaultHttpClient httpclient = new DefaultHttpClient();
        final BufferedReader prompt = new BufferedReader(new InputStreamReader(System.in));

        CorfuConfiguration C = null;

        while (true) {

            System.out.print("> ");
            String line = prompt.readLine();
            if (line.startsWith("get")) {

                HttpGet httpget = new HttpGet("http://localhost:8000/corfu");

                System.out.println("Executing request: " + httpget.getRequestLine());
                HttpResponse response = (HttpResponse) httpclient.execute(httpget);

                System.out.println("----------------------------------------");
                System.out.println(response.getStatusLine());
                // response.getEntity().writeTo(System.out);
                // System.out.println();
                // System.out.println("----------------------------------------");

                C = new CorfuConfiguration(response.getEntity().getContent());
            } else {

                if (C == null) {
                    System.out.println("configuration not set yet!");
                    continue;
                }

                C.changeEpoch(C.getGlobalEpoch() + 1);

                HttpPost httppost = new HttpPost("http://localhost:8000/corfu");
                httppost.setEntity(new StringEntity(C.ConfToXMLString()));

                System.out.println("Executing request: " + httppost.getRequestLine());
                HttpResponse response = httpclient.execute(httppost);

                System.out.println("----------------------------------------");
                System.out.println(response.getStatusLine());
                response.getEntity().writeTo(System.out);

            }
        }
        // httpclient.close();
    }
}

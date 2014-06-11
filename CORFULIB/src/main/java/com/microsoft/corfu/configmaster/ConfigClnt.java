package com.microsoft.corfu.configmaster;

import com.microsoft.corfu.CorfuConfiguration;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.*;

/**
 * Created by dalia on 6/9/2014.
 */
public class ConfigClnt {
    public static void main(String[] args) throws IOException {

        CloseableHttpClient httpclient = HttpClients.createDefault();
        final BufferedReader prompt = new BufferedReader(new InputStreamReader(System.in));

        CorfuConfiguration C = null;

        while (true) {

            try {
                System.out.print("> ");
                String line = prompt.readLine();
                if (line.startsWith("get")) {

                    HttpGet httpget = new HttpGet("http://localhost:8000/corfu");

                    System.out.println("Executing request: " + httpget.getRequestLine());
                    CloseableHttpResponse response = httpclient.execute(httpget);

                    System.out.println("----------------------------------------");
                    System.out.println(response.getStatusLine());
                    // response.getEntity().writeTo(System.out);
                    // System.out.println();
                    // System.out.println("----------------------------------------");

                    C = new CorfuConfiguration(response.getEntity().getContent());
                } else {

                    if (C == null) {
                        System.out.println("configuration not set yet!"); continue;
                    }

                    C.changeEpoch(C.getGlobalEpoch()+1);

                    HttpPost httppost = new HttpPost("http://localhost:8000/corfu");
                    httppost.setEntity(new StringEntity(C.ConfToXMLString()));

                    System.out.println("Executing request: " + httppost.getRequestLine());
                    CloseableHttpResponse response = httpclient.execute(httppost);

                    System.out.println("----------------------------------------");
                    System.out.println(response.getStatusLine());
                    response.getEntity().writeTo(System.out);

                }
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            } catch (TransformerException e) {
                e.printStackTrace();
            }
        }
        // httpclient.close();
    }
}

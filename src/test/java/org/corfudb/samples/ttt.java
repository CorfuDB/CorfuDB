package org.corfudb.samples;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.File;
import java.util.List;

/**
 * Created by dmalkhi on 11/18/15.
 */
public class TTT {
    public static void main(String[] args) {
        JsonObject obj = Json.createObjectBuilder().build();

        System.out.println("obj.get(test): " + obj.getJsonObject("test"));
        if (obj.get("test") == null) System.out.println("it is null!");
        System.out.println("obj.isNull(test): " + obj.isNull("test")) ;


    }
}

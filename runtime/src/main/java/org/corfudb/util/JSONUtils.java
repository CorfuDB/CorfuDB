package org.corfudb.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

/**
 * Created by mdhawan on 6/28/16.
 */
public class JSONUtils {
    public static final Gson parser = new GsonBuilder().setPrettyPrinting()
                                            .create();

    public static String prettyPrint(String jsonString) {
        JsonParser p = new JsonParser();
        JsonElement e = p.parse(jsonString);
        return parser.toJson(e);
    }
}

package org.corfudb.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

/**
 * Created by mdhawan on 6/28/16.
 */
public class JsonUtils {
    public static final Gson prettyParser = new GsonBuilder().setPrettyPrinting().create();
    public static final Gson parser = new GsonBuilder().create();


    /**
     * Return an intedented json string.
     * @param jsonString Json String
     * @return           Pretty json string
     */
    public static String prettyPrint(String jsonString) {
        JsonParser p = new JsonParser();
        JsonElement e = p.parse(jsonString);
        return prettyParser.toJson(e);
    }

    public static <T> String toJsonString(T obj){
        return parser.toJson(obj);
    }
}

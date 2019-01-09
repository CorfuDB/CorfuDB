package org.corfudb.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

/**
 * Created by mdhawan on 6/28/16.
 */
public class JsonUtils {
    public static final Gson parser = new GsonBuilder().setPrettyPrinting().create();
    private static final Gson REGULAR_PARSER = new GsonBuilder().create();


    /**
     * Return an intedented json string.
     * @param jsonString Json String
     * @return           Pretty json string
     */
    public static String prettyPrint(String jsonString) {
        JsonParser p = new JsonParser();
        JsonElement e = p.parse(jsonString);
        return parser.toJson(e);
    }

    public static <T> String toJson(T obj){
        return REGULAR_PARSER.toJson(obj);
    }
}

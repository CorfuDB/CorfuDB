package org.corfudb.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Created by mdhawan on 6/28/16.
 */
public class JSONUtils {
    public static final Gson parser = new GsonBuilder().create();
}

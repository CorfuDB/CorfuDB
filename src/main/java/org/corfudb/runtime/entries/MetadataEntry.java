package org.corfudb.runtime.entries;

import java.util.HashMap;

/**
 * Created by taia on 6/17/15.
 */
public class MetadataEntry {
    private HashMap<String, Object> map;

    // Some common fields
    public static String NEXT = "next";
    public static String DECISION = "decision";
    public static String TXN_COMMAND = "multicommand";


    public MetadataEntry() {
        map = new HashMap<String, Object>();
    }

    public Object getMetadata(String s) {
        return map.get(s);
    }

    public HashMap<String, Object> getMetadataMap() {
        return map;
    }

    public Object writeMetadata(String s, Object o) {
        return map.put(s, o);
    }
}

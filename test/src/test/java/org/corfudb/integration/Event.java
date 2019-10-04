package org.corfudb.integration;
/*
message EventInfo {
    optional uint32 id = 1;
    optional string name = 2;
    optional uint32 port = 3;
    optional int64 event_time = 4 [(org.corfudb.runtime.schema).secondary_key = true];
    optional uint32 frequency = 5;
 */

public class Event {
    private int id;
    private String name;
    private int port;
    private long event_time;
    private int frequency;

    public Event(int id, String name, int port, long event_time, int freq) {
        this.id = id;
        this.name = name;
        this.port = port;
        this.event_time = event_time;
        this.frequency = freq;
    }
}
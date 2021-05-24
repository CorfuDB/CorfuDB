package org.corfudb.integration;
/*
message EventInfo {
    optional uint32 id = 1;
    optional string name = 2;
    optional uint32 port = 3;
    optional int64 event_time = 4 [(org.corfudb.runtime.schema).secondary_key = true];
    optional uint32 frequency = 5;
 */

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Event {
    private int id;
    private String name;
    private int port;
    private long event_time;
    private int frequency;
}
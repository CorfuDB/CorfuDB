package org.corfudb.protocols.logprotocol;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.Map;
import java.util.UUID;

/**
 * Created by sfritchie on 4/6/17.
 */
@ToString(callSuper = true)
////////////////// @AllArgsConstructor
public class CheckpointEntry extends LogEntry {

    @RequiredArgsConstructor
    public enum CheckpointEntryType {
        START(1),           // Mandatory: 1st record in checkpoint
        CONTINUATION(2),    // Optional: 2nd through (n-1)th record
        END(3),             // Mandatory: for successful checkpoint
        FAIL(4);            // Optional: external party declares this checkpoint has failed

        public final int type;

        public byte asByte() {
            return (byte) type;
        }
    };

    CheckpointEntryType type;
    UUID checkpointID;  // Unique identifier for this checkpoint
    String checkpointAuthorID;  // TODO: UUID instead?
    Map<String,String> dict;
    byte[] bulk;

    public CheckpointEntry(CheckpointEntryType type, String authorID, UUID checkpointID,
                           Map<String,String> dict, byte[] bulk) {
        super(LogEntryType.CHECKPOINT);
        this.type = type;
        this.checkpointID = checkpointID;
        this.checkpointAuthorID = authorID;
        this.dict = dict;
        this.bulk = bulk;
    }

    public static int hello = 42;
}

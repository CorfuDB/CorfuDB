package org.corfudb.infrastructure.logreplication.runtime.fsm;

import lombok.Data;

import java.util.UUID;

/**
 * This class represents a Communication Event
 */
@Data
public class CommunicationEvent {
    /**
     * Enum listing the various type of CommunicationEvent.
     */
    public enum CommunicationEventType {
        CONNECT,
        CONNECTION_UP,
        CONNECTION_DOWN,
        LEADER_NOT_FOUND,
        LEADER_FOUND,
        NEGOTIATE_COMPLETE,
        NEGOTIATE_FAILED,
        SHUTDOWN
    }

    /*
     * Communication Event Identifier
     */
    private UUID eventID;

    /*
     * Communication Event Type
     */
    private CommunicationEventType type;

    /*
     * Endpoint associated to this communication event.
     */
    private String endpoint;

    /**
     * Constructor
     *
     * @param type communication event type
     */
    public CommunicationEvent(CommunicationEventType type, String endpoint) {
        this.type = type;
        this.endpoint = endpoint;
        this.eventID = UUID.randomUUID();
    }

    /**
     * Constructor
     *
     * @param type communication event type
     */
    public CommunicationEvent(CommunicationEventType type) {
        this(type, null);
    }

}

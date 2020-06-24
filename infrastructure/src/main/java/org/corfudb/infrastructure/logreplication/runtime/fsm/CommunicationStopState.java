package org.corfudb.infrastructure.logreplication.runtime.fsm;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommunicationStopState implements CommunicationState {

    public CommunicationStopState() {}

    @Override
    public CommunicationStateType getType() {
        return CommunicationStateType.STOP;
    }

    @Override
    public CommunicationState processEvent(CommunicationEvent event) throws IllegalTransitionException {
        switch (event.getType()) {
            case SHUTDOWN:
                return this;
            default: {
                log.warn("Unexpected communication event {} when in init state.", event.getType());
                throw new IllegalTransitionException(event.getType(), getType());
            }
        }
    }
}

package org.corfudb.infrastructure.logreplication.runtime.fsm;

import lombok.extern.slf4j.Slf4j;

/**
 * Log Replication Runtime Unrecoverable State.
 *
 * We reach this state if an unrecoverable error occurred and log replication cannot be triggered or continued.
 *
 * @author amartinezman
 */
@Slf4j
public class UnrecoverableState implements LogReplicationRuntimeState {

    public UnrecoverableState() {}

    @Override
    public LogReplicationRuntimeStateType getType() {
        return LogReplicationRuntimeStateType.UNRECOVERABLE;
    }

    @Override
    public LogReplicationRuntimeState processEvent(LogReplicationRuntimeEvent event) throws IllegalTransitionException {
        switch (event.getType()) {
            case ERROR:
                return this;
            default: {
                log.warn("Unexpected communication event {} when in init state.", event.getType());
                throw new IllegalTransitionException(event.getType(), getType());
            }
        }
    }

    @Override
    public void onEntry(LogReplicationRuntimeState from) {
        // TODO: any specific action once determined there is an unrecoverable error?
        //  shutdown connections?, notify cluster manager?
    }

    /**
     * Set throwable cause to take this runtime to an unrecoverable state.
     * @param cause
     */
    public void setThrowableCause(Throwable cause) {
        log.error("Log Replication will NOT continue. Entered UNRECOVERABLE state, cause={}", cause);
    }
}

package org.corfudb.infrastructure.logreplication.replication.fsm;

import lombok.Data;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;

import java.util.Observable;

/**
 * This class represents an observable value which reflects the number of
 * received messages and the last received LogReplicationEntry, i.e.,
 * an object that the application will observe
 * to receive notifications on change.
 */
@Data
public class ObservableAckMsg extends Observable
{
    int msgCnt;
    LogReplicationEntry dataMessage;

    public ObservableAckMsg() {
        this.msgCnt = 0;
        this.dataMessage = null;
    }

    public void setValue(LogReplicationEntry dataMessage)
    {
        if (dataMessage != null) {
            this.dataMessage = dataMessage;
            msgCnt++;
        }
        setChanged();
        notifyObservers(dataMessage);
    }

    public ObservableAckMsg getValue()
    {
        return this;
    }
}
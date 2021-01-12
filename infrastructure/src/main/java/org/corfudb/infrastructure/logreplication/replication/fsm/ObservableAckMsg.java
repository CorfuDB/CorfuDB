package org.corfudb.infrastructure.logreplication.replication.fsm;

import lombok.Data;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.view.Address;

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
    private int msgCnt;
    private LogReplicationEntryMsg dataMessage;
    private long lastAckedTs;

    public ObservableAckMsg() {
        this.msgCnt = 0;
        this.dataMessage = null;
        this.lastAckedTs = Address.NON_ADDRESS;
    }

    public void setValue(LogReplicationEntryMsg dataMessage)
    {
        if (dataMessage != null) {
            this.dataMessage = dataMessage;
            // Omit ACKs for resent messages
            if (dataMessage.getMetadata().getTimestamp() > lastAckedTs) {
                this.lastAckedTs = dataMessage.getMetadata().getTimestamp();
                msgCnt++;
            }
        }
        setChanged();
        notifyObservers(dataMessage);
    }

    public ObservableAckMsg getValue()
    {
        return this;
    }
}
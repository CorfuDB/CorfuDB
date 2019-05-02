package org.corfudb.runtime.object;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.corfudb.protocols.logprotocol.ISMREntryLocator;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.util.serializer.ISerializer;

//TODO(Xin): SMREntryWithLocatior should be a wrapper, not a subclass.
public class SMREntryWithLocator extends SMREntry {
    @Getter
    @Setter
    private final ISMREntryLocator locator;

    private final SMREntry smrEntry;

    public SMREntryWithLocator(SMREntry smrEntry, ISMREntryLocator locator) {
        super(smrEntry);
        this.smrEntry = smrEntry;
        this.locator = locator;
    }


    @Override
    public String getSMRMethod() {
        return smrEntry.getSMRMethod();
    }

    @Override
    public Object[] getSMRArguments() {
        return smrEntry.getSMRArguments();
    }

    @Override
    public ISerializer getSerializerType() {
        return smrEntry.getSerializerType();
    }

    @Override
    public Object getUndoRecord() {
        return smrEntry.getUndoRecord();
    }

    @Override
    public boolean isUndoable() {
        return smrEntry.isUndoable();
    }

    @Override
    public Object getUpcallResult() {
        return smrEntry.getUpcallResult();
    }

    @Override
    public boolean isHaveUpcallResult() {
        return smrEntry.isHaveUpcallResult();
    }

    @Override
    public void setUpcallResult(Object result) {
        smrEntry.setUpcallResult(result);
    }

    @Override
    public void setUndoRecord(Object object) {
        smrEntry.setUndoRecord(object);
    }

    public void clearUndoRecord() {
        smrEntry.clearUndoRecord();
    }


}

package org.corfudb.runtime.object;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.corfudb.protocols.logprotocol.ISMREntryLocator;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.util.serializer.ISerializer;

public class SMREntryWithLocator extends SMREntry {
    @Getter
    @Setter
    ISMREntryLocator locator;

    public SMREntryWithLocator(String smrMethod, @NonNull Object[] smrArguments, ISerializer serializer,
                               ISMREntryLocator locator) {
        super(smrMethod, smrArguments, serializer);
        this.locator = locator;
    }

    public SMREntryWithLocator(SMREntry smrEntry, ISMREntryLocator locator) {
        super(smrEntry);
        this.locator = locator;
    }
}

package org.corfudb.protocols.logprotocol;

import com.google.common.reflect.TypeToken;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum SMREntryType {
    SMREntry(0, TypeToken.of(SMREntry.class)),
    MultiSMREntry(1, TypeToken.of(MultiSMREntry.class)),
    MultiObjectSMREntry(2, TypeToken.of(MultiObjectSMREntry.class));

    final int type;
    TypeToken<? extends ISMRConsumable> smrEntryType;

    public byte asByte() {
        return (byte) type;
    }


}

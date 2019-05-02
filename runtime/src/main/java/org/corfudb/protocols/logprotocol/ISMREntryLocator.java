package org.corfudb.protocols.logprotocol;

public interface ISMREntryLocator extends Comparable<ISMREntryLocator>{
    long getGlobalAddress();
    long getIndex();
}

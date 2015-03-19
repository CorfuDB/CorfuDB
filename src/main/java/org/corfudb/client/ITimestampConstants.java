package org.corfudb.client;

public interface ITimestampConstants
{
    ITimestamp getInvalidTimestamp();
    ITimestamp getMaxTimestamp();
    ITimestamp getMinTimestamp();
}
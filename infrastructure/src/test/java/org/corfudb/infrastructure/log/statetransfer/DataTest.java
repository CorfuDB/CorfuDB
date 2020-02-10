package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.collect.Ordering;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class DataTest {
    public LogData createStubRecord(long address) {
        LogData data = mock(LogData.class);
        doReturn(address).when(data).getGlobalAddress();
        return data;
    }

    public List<LogData> createStubList(List<Long> addresses) {
        return addresses.stream().map(this::createStubRecord).collect(Collectors.toList());
    }

    public Map<Long, ILogData> createStubMap(List<LogData> data) {
        return data.stream().map(rec -> new AbstractMap.SimpleEntry<>(rec.getGlobalAddress(), (ILogData) rec))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }

    public Map<Long, ILogData> createStubMapFromLongs(List<Long> data){
        return data.stream().map(add -> new AbstractMap.SimpleEntry<>(add, createStubRecord(add)))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }

    public List<LogData> getRecordsFromStubMap(Map<Long, ILogData> stubMap) {
        return Ordering.natural().sortedCopy(stubMap.values().stream().map(x -> (LogData) x).collect(Collectors.toList()));
    }

}

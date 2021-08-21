package org.corfudb.runtime;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.compression.Codec;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTable;
import static org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTableSMRMethods.CLEAR;
import static org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTableSMRMethods.DELETE;
import static org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTableSMRMethods.UPDATE;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.SequencerView;
import org.corfudb.runtime.view.remotecorfutable.RemoteCorfuTableView;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * This class contains unit tests for the Remote Corfu Table.
 *
 * Created by nvaishampayan517 on 08/19/21
 */
@Slf4j
public class RemoteCorfuTableTest {

    @Rule
    public MockitoRule mockito = MockitoJUnit.rule();

    //table to test
    private RemoteCorfuTable<String, Integer> table;

    //objects to mock
    private CorfuRuntime mRuntime;
    private CorfuRuntime.CorfuRuntimeParameters mRuntimeParams;
    private AddressSpaceView mAddressSpaceView;
    private SequencerView mSequencerView;
    private RemoteCorfuTableView mRemoteCorfuTableView;
    private TokenResponse mTokenResponse;
    private Token mToken;

    private final ISerializer serializer = Serializers.getDefaultSerializer();

    @Before
    public void setup() {
        mRuntime = mock(CorfuRuntime.class);
        mRuntimeParams = mock(CorfuRuntime.CorfuRuntimeParameters.class);
        mAddressSpaceView = mock(AddressSpaceView.class);
        mSequencerView = mock(SequencerView.class);
        mRemoteCorfuTableView = mock(RemoteCorfuTableView.class);
        mTokenResponse = mock(TokenResponse.class);
        mToken = mock(Token.class);

        when(mRuntime.getAddressSpaceView()).thenReturn(mAddressSpaceView);
        when(mRuntime.getSequencerView()).thenReturn(mSequencerView);
        when(mRuntime.getRemoteCorfuTableView()).thenReturn(mRemoteCorfuTableView);
        when(mRuntime.getParameters()).thenReturn(mRuntimeParams);

        when(mAddressSpaceView.getLogTail()).thenReturn(1000L);

        when(mSequencerView.next(any(UUID.class))).thenReturn(mTokenResponse);

        when(mRuntimeParams.getCodecType()).thenReturn(Codec.Type.NONE);
        when(mRuntimeParams.getMaxWriteSize()).thenReturn(Integer.MAX_VALUE);
        when(mRuntimeParams.getWriteRetry()).thenReturn(1);

        when(mTokenResponse.getToken()).thenReturn(mToken);

        when(mToken.getSequence()).thenReturn(2000L);

        table = RemoteCorfuTable.RemoteCorfuTableFactory.openTable(mRuntime, "testTable");
    }

    @Test
    public void testInsert() {
        String testKey = "test";
        Integer testValue = 1;

        ArgumentCaptor<LogData> logDataCaptor = ArgumentCaptor.forClass(LogData.class);
        table.insert(testKey, testValue);

        //capture data write
        verify(mAddressSpaceView).write(any(TokenResponse.class), logDataCaptor.capture());
        LogData dataToWrite = logDataCaptor.getValue();

        //extract SMREntry from ILogData
        byte[] serializedData = dataToWrite.getData();
        ByteBuf serializedSMREntry = Unpooled.wrappedBuffer(serializedData);
        SMREntry entry = (SMREntry) LogEntry.deserialize(serializedSMREntry, mRuntime);

        assertEquals(UPDATE.getSMRName(),entry.getSMRMethod());
        assertEquals(2, entry.getSMRArguments().length);
        assertEquals(testKey, entry.getSMRArguments()[0]);
        assertEquals(testValue, entry.getSMRArguments()[1]);
    }

    @Test
    public void testDelete() {
        String testKey = "test";
        ArgumentCaptor<LogData> logDataCaptor = ArgumentCaptor.forClass(LogData.class);
        table.delete(testKey);

        //capture data write
        verify(mAddressSpaceView).write(any(TokenResponse.class), logDataCaptor.capture());
        LogData dataToWrite = logDataCaptor.getValue();

        //extract SMREntry from ILogData
        byte[] serializedData = dataToWrite.getData();
        ByteBuf serializedSMREntry = Unpooled.wrappedBuffer(serializedData);
        SMREntry entry = (SMREntry) LogEntry.deserialize(serializedSMREntry, mRuntime);

        assertEquals(DELETE.getSMRName(),entry.getSMRMethod());
        assertEquals(1, entry.getSMRArguments().length);
        assertEquals(testKey, entry.getSMRArguments()[0]);
    }

    @Test
    public void testMultiDelete() {
        List<String> testKeys = new ArrayList<>(5);
        for (int i = 0; i < 5; i++) {
            testKeys.add("testKeys" + i);
        }

        ArgumentCaptor<LogData> logDataCaptor = ArgumentCaptor.forClass(LogData.class);
        table.multiDelete(testKeys);

        //capture data write
        verify(mAddressSpaceView).write(any(TokenResponse.class), logDataCaptor.capture());
        LogData dataToWrite = logDataCaptor.getValue();

        //extract SMREntry from ILogData
        byte[] serializedData = dataToWrite.getData();
        ByteBuf serializedSMREntry = Unpooled.wrappedBuffer(serializedData);
        SMREntry entry = (SMREntry) LogEntry.deserialize(serializedSMREntry, mRuntime);

        assertEquals(DELETE.getSMRName(),entry.getSMRMethod());
        assertEquals(5, entry.getSMRArguments().length);
        for (int i = 0; i < 5; i++) {
            assertEquals(testKeys.get(i), entry.getSMRArguments()[i]);
        }
    }

    @Test
    public void testPutAll() {
        Map<String, Integer> testMap = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            testMap.put("key" + i, i);
        }

        ArgumentCaptor<LogData> logDataCaptor = ArgumentCaptor.forClass(LogData.class);
        table.putAll(testMap);

        //capture data write
        verify(mAddressSpaceView).write(any(TokenResponse.class), logDataCaptor.capture());
        LogData dataToWrite = logDataCaptor.getValue();

        //extract SMREntry from ILogData
        byte[] serializedData = dataToWrite.getData();
        ByteBuf serializedSMREntry = Unpooled.wrappedBuffer(serializedData);
        SMREntry entry = (SMREntry) LogEntry.deserialize(serializedSMREntry, mRuntime);

        assertEquals(UPDATE.getSMRName(), entry.getSMRMethod());
        assertEquals(10, entry.getSMRArguments().length);

        Multiset<RemoteCorfuTable.RemoteCorfuTableEntry<String,Integer>> expectedSet = ImmutableMultiset.copyOf(
                testMap.entrySet().stream()
                        .map(mapEntry ->
                                new RemoteCorfuTable.RemoteCorfuTableEntry<>(mapEntry.getKey(), mapEntry.getValue()))
                        .collect(Collectors.toList()));

        List<RemoteCorfuTable.RemoteCorfuTableEntry<String , Integer>> readList = new LinkedList<>();
        for (int i = 0; i < entry.getSMRArguments().length; i += 2) {
            readList.add(new RemoteCorfuTable.RemoteCorfuTableEntry<>(
                    (String) entry.getSMRArguments()[i],(Integer) entry.getSMRArguments()[i+1]));
        }
        Multiset<RemoteCorfuTable.RemoteCorfuTableEntry<String,Integer>> readSet = ImmutableMultiset.copyOf(readList);
        assertEquals(expectedSet, readSet);
    }

    @Test
    public void testUpdateAll() {
        List<RemoteCorfuTable.RemoteCorfuTableEntry<String, Integer>> entries = new ArrayList<>(5);
        for (int i = 0; i < 5; i++) {
            entries.add(new RemoteCorfuTable.RemoteCorfuTableEntry<>("key" + i, i));
        }

        ArgumentCaptor<LogData> logDataCaptor = ArgumentCaptor.forClass(LogData.class);
        table.updateAll(entries);

        //capture data write
        verify(mAddressSpaceView).write(any(TokenResponse.class), logDataCaptor.capture());
        LogData dataToWrite = logDataCaptor.getValue();

        //extract SMREntry from ILogData
        byte[] serializedData = dataToWrite.getData();
        ByteBuf serializedSMREntry = Unpooled.wrappedBuffer(serializedData);
        SMREntry entry = (SMREntry) LogEntry.deserialize(serializedSMREntry, mRuntime);

        assertEquals(UPDATE.getSMRName(), entry.getSMRMethod());
        assertEquals(10, entry.getSMRArguments().length);
        for (int i = 0; i < entries.size(); i++) {
            RemoteCorfuTable.RemoteCorfuTableEntry<String, Integer> readEntry =
                    new RemoteCorfuTable.RemoteCorfuTableEntry<>(
                            (String) entry.getSMRArguments()[2*i], (Integer) entry.getSMRArguments()[2*i+1]);
            assertEquals(entries.get(i), readEntry);
        }
    }

    @Test
    public void clear() {
        ArgumentCaptor<LogData> logDataCaptor = ArgumentCaptor.forClass(LogData.class);
        table.clear();

        //capture data write
        verify(mAddressSpaceView).write(any(TokenResponse.class), logDataCaptor.capture());
        LogData dataToWrite = logDataCaptor.getValue();

        //extract SMREntry from ILogData
        byte[] serializedData = dataToWrite.getData();
        ByteBuf serializedSMREntry = Unpooled.wrappedBuffer(serializedData);
        SMREntry entry = (SMREntry) LogEntry.deserialize(serializedSMREntry, mRuntime);

        assertEquals(CLEAR.getSMRName(), entry.getSMRMethod());
        assertEquals(0, entry.getSMRArguments().length);
    }
}

package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.protocols.logprotocol.SMREntryGarbageInfo;
import org.corfudb.protocols.logprotocol.SMRLogEntry;
import org.corfudb.protocols.logprotocol.SMRRecord;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class LogDataTest {
    @Test
    public void testSMRPayloadSerializeDeserialize() {
        String smrMethod = "put";
        Object[] smrArguments = new Object[] {"a"};
        ISerializer iSerializer = Serializers.JSON;

        SMRLogEntry smrLogEntry = new SMRLogEntry();
        SMRRecord smrRecord = new SMRRecord(smrMethod, smrArguments, iSerializer);
        UUID streamId = CorfuRuntime.getStreamID("stream1");
        smrLogEntry.addTo(streamId, smrRecord);
        LogData logData = new LogData(DataType.DATA, smrLogEntry);
        ByteBuf buf = Unpooled.buffer();
        logData.doSerialize(buf);
        LogData deserializedLogData = new LogData(buf);

        // TODO(Xin): runtime is not used in the test. Future patch will eliminate the dependency on runtime.
        SMRLogEntry deserializedSMREntry = (SMRLogEntry) deserializedLogData.getPayload(null);
        SMRRecord deserializedSMRRecord = deserializedSMREntry.getSMRUpdates(streamId).get(0);

        Assert.assertEquals(smrRecord.getSMRMethod(), deserializedSMRRecord.getSMRMethod());
        Assert.assertEquals(smrRecord.getSMRArguments(), deserializedSMRRecord.getSMRArguments());
        Assert.assertEquals(smrRecord.getSerializerType(), deserializedSMRRecord.getSerializerType());
    }

    @Test
    public void testGarbagePayloadSerializeDeserialize() {
        Long detectedAddress = 0L;
        int smrEntrySize = 0;

        SMREntryGarbageInfo smrEntryGarbageInfo = new SMREntryGarbageInfo(detectedAddress, smrEntrySize);
        LogData logData = new LogData(DataType.GARBAGE, smrEntryGarbageInfo);
        ByteBuf buf = Unpooled.buffer();
        logData.doSerialize(buf);
        LogData deserializedLogData = new LogData(buf);

        // TODO(Xin): runtime is not used in the test. Future patch will eliminate the dependency on runtime.
        SMREntryGarbageInfo deserializedSMREntryGarbageInfo =
                (SMREntryGarbageInfo) deserializedLogData.getPayload(null);


        Assert.assertEquals(detectedAddress, (Long) deserializedSMREntryGarbageInfo.getDetectorAddress());
        Assert.assertEquals(smrEntrySize, deserializedSMREntryGarbageInfo.getGarbageSize());
    }
}

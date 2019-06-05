package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.logprotocol.SMREntryGarbageInfo;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Assert;
import org.junit.Test;

public class LogDataTest {
    @Test
    public void testSMRPayloadSerializeDeserialize() {
        String smrMethod = "put";
        Object[] smrArguments = new Object[] {"a"};
        ISerializer iSerializer = Serializers.JSON;

        SMREntry smrEntry = new SMREntry(smrMethod, smrArguments, iSerializer);
        LogData logData = new LogData(DataType.DATA, smrEntry);
        ByteBuf buf = Unpooled.buffer();
        logData.doSerialize(buf);
        LogData deserializedLogData = new LogData(buf);

        // TODO(Xin): runtime is not used in the test. Future patch will eliminate the dependency on runtime.
        SMREntry deserializedSMREntry = (SMREntry) deserializedLogData.getPayload(null);

        Assert.assertEquals(smrEntry.getSMRMethod(), deserializedSMREntry.getSMRMethod());
        Assert.assertEquals(smrEntry.getSMRArguments(), deserializedSMREntry.getSMRArguments());
        Assert.assertEquals(smrEntry.getSerializerType(), deserializedSMREntry.getSerializerType());
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

package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;


public class LogEntryTest {
    private static UUID streamId;

    private static SMREntry smrEntry;
    private static MultiSMREntry multiSMREntry;
    private static MultiObjectSMREntry multiObjectSMREntry;

    private static SMREntryGarbageInfo smrEntryGarbageInfo;
    private static MultiSMREntryGarbageInfo multiSMREntryGarbageInfo;
    private static MultiObjectSMREntryGarbageInfo multiObjectSMREntryGarbageInfo;

    @BeforeClass
    public static void setUpLogEntryInstances() {
        String smrMethod = "put";
        Object[] smrArguments = new Object[]{"a"};
        ISerializer serializerType = Serializers.JSON;
        streamId = UUID.randomUUID();

        smrEntry = new SMREntry(smrMethod, smrArguments, serializerType);
        multiSMREntry = new MultiSMREntry();
        multiSMREntry.addTo(smrEntry);
        multiObjectSMREntry = new MultiObjectSMREntry();
        multiObjectSMREntry.addTo(streamId, smrEntry);

        long detectedAddress = 0L;
        int smrEntrySize = 0;

        smrEntryGarbageInfo = new SMREntryGarbageInfo(detectedAddress, smrEntrySize);
        multiSMREntryGarbageInfo = new MultiSMREntryGarbageInfo();
        multiSMREntryGarbageInfo.add(streamId, 0, smrEntryGarbageInfo);
        multiObjectSMREntryGarbageInfo = new MultiObjectSMREntryGarbageInfo();
        multiObjectSMREntryGarbageInfo.add(streamId, 0, smrEntryGarbageInfo);
    }

    @Test
    public void testSMREntrySerializeDeserialize() {
        ByteBuf buf = Unpooled.buffer();
        smrEntry.serialize(buf);

        // TODO(Xin): runtime is not used in the test. Future patch will eliminate the dependency on runtime.
        SMREntry deserializedSMREntry = (SMREntry) LogEntry.deserialize(buf, null);

        assertSMRConsumableEquals(smrEntry, deserializedSMREntry, UUID.randomUUID());
    }

    @Test
    public void testMultiSMREntrySerializeDeserialize() {
        ByteBuf buf = Unpooled.buffer();
        multiSMREntry.serialize(buf);

        // TODO(Xin): runtime is not used in the test. Future patch will eliminate the dependency on runtime.
        MultiSMREntry deserializedMultiSMREntry = (MultiSMREntry) LogEntry.deserialize(buf, null);

        assertSMRConsumableEquals(multiSMREntry, deserializedMultiSMREntry, streamId);
    }

    @Test
    public void testMultiObjectSMREntrySerializeDeserialize() {
        ByteBuf buf = Unpooled.buffer();
        multiObjectSMREntry.serialize(buf);

        // TODO(Xin): runtime is not used in the test. Future patch will eliminate the dependency on runtime.
        MultiObjectSMREntry deserializedMultiObjectSMREntry = (MultiObjectSMREntry) LogEntry.deserialize(buf, null);

        assertSMRConsumableEquals(multiObjectSMREntry, deserializedMultiObjectSMREntry, streamId);
    }

    @Test
    public void testSMREntryGarbageInfoSerializeDeserialize() {
        ByteBuf buf = Unpooled.buffer();
        smrEntryGarbageInfo.serialize(buf);

        // TODO(Xin): runtime is not used in the test. Future patch will eliminate the dependency on runtime.
        SMREntryGarbageInfo deserializedSMREntryGarbageInfo = (SMREntryGarbageInfo) LogEntry.deserialize(buf, null);
        assertSMRGarbageInfoEquals(smrEntryGarbageInfo, deserializedSMREntryGarbageInfo);
    }

    @Test
    public void testMultiSMREntryGarbageInfoSerializeDeserialize() {
        ByteBuf buf = Unpooled.buffer();
        multiSMREntryGarbageInfo.serialize(buf);

        // TODO(Xin): runtime is not used in the test. Future patch will eliminate the dependency on runtime.
        MultiSMREntryGarbageInfo deserializedMultiSMREntryGarbageInfo =
                (MultiSMREntryGarbageInfo) LogEntry.deserialize(buf, null);
        assertSMRGarbageInfoEquals(multiSMREntryGarbageInfo, deserializedMultiSMREntryGarbageInfo);
    }

    @Test
    public void testMultiSMRObjectEntryGarbageInfoSerializeDeserialize() {
        ByteBuf buf = Unpooled.buffer();
        multiObjectSMREntryGarbageInfo.serialize(buf);

        // TODO(Xin): runtime is not used in the test. Future patch will eliminate the dependency on runtime.
        MultiObjectSMREntryGarbageInfo deserializedSMRMultiObjectEntryGarbageInfo =
                (MultiObjectSMREntryGarbageInfo) LogEntry.deserialize(buf, null);
        assertSMRGarbageInfoEquals(multiObjectSMREntryGarbageInfo, deserializedSMRMultiObjectEntryGarbageInfo);
    }


    private void assertSMRConsumableEquals(ISMRConsumable expectSMRConsumable, ISMRConsumable testedSMRConsumable,
                                          UUID streamId) {
        List<SMREntry> expectedUpdates = expectSMRConsumable.getSMRUpdates(streamId);
        List<SMREntry> testedUpdates = testedSMRConsumable.getSMRUpdates(streamId);

        Assert.assertEquals(expectedUpdates.size(), testedUpdates.size());

        for (int i = 0; i < expectedUpdates.size(); ++i) {
            SMREntry expectedSMREntry = expectedUpdates.get(i);
            SMREntry testedSMREntry = testedUpdates.get(i);

            Assert.assertEquals(expectedSMREntry.getSMRMethod(), testedSMREntry.getSMRMethod());
            Assert.assertEquals(expectedSMREntry.getSMRArguments(), testedSMREntry.getSMRArguments());
            Assert.assertEquals(expectedSMREntry.getSerializerType(), testedSMREntry.getSerializerType());
        }
    }

    private void assertSMRGarbageInfoEquals(ISMRGarbageInfo expectedSMRGarbageInfo,
                                            ISMRGarbageInfo testedSMRGarbageInfo) {
        Map<Integer, SMREntryGarbageInfo> expectedGarbageMap = expectedSMRGarbageInfo.getAllGarbageInfo(streamId);
        Map<Integer, SMREntryGarbageInfo> testedGarbageMap = testedSMRGarbageInfo.getAllGarbageInfo(streamId);

        Assert.assertEquals(expectedGarbageMap.size(), testedGarbageMap.size());

        for (int index : expectedGarbageMap.keySet()) {
            SMREntryGarbageInfo expectedGarbageInfo = expectedGarbageMap.get(index);

            Assert.assertTrue(testedGarbageMap.containsKey(index));
            SMREntryGarbageInfo testedGarbageInfo = testedGarbageMap.get(index);

            Assert.assertEquals(expectedGarbageInfo.getDetectorAddress(), testedGarbageInfo.getDetectorAddress());
        }
    }
}

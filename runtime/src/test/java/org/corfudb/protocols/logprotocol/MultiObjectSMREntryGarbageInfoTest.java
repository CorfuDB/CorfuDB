package org.corfudb.protocols.logprotocol;

import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class MultiObjectSMREntryGarbageInfoTest {
    private static UUID streamId1 = UUID.randomUUID();
    private static long address1_1 = 1L;
    private static int size1_1 = 100;
    private static int index1_1 = 0;

    private static UUID streamId2 = UUID.randomUUID();
    private static long address2_1 = 1L;
    private static int size2_1 = 200;
    private static int index2_1 = 2;
    private static long address2_2 = 2L;
    private static int size2_2 = 300;
    private static int index2_2 = 5;

    private void setUpInstance(MultiObjectSMREntryGarbageInfo gcInfo) {
        SMREntryGarbageInfo smrEntryGarbageInfo1_1 = new SMREntryGarbageInfo(address1_1, size1_1);
        SMREntryGarbageInfo smrEntryGarbageInfo2_1 = new SMREntryGarbageInfo(address2_1, size2_1);
        SMREntryGarbageInfo smrEntryGarbageInfo2_2 = new SMREntryGarbageInfo(address2_2, size2_2);

        gcInfo.add(streamId1, index1_1, smrEntryGarbageInfo1_1);
        gcInfo.add(streamId2, index2_1, smrEntryGarbageInfo2_1);
        gcInfo.add(streamId2, index2_2, smrEntryGarbageInfo2_2);
    }

    @Test
    public void testGetGarbageSize() {
        MultiObjectSMREntryGarbageInfo gcInfo = new MultiObjectSMREntryGarbageInfo();
        setUpInstance(gcInfo);
        Assert.assertEquals(size1_1 + size2_1 + size2_2, gcInfo.getGarbageSize());
    }

    @Test
    public void testEquals() {
        MultiObjectSMREntryGarbageInfo gcInfo = new MultiObjectSMREntryGarbageInfo();
        setUpInstance(gcInfo);

        MultiObjectSMREntryGarbageInfo empty = new MultiObjectSMREntryGarbageInfo();
        Assert.assertNotEquals(empty, gcInfo);

        MultiObjectSMREntryGarbageInfo other = new MultiObjectSMREntryGarbageInfo();
        other.add(streamId1, index1_1, new SMREntryGarbageInfo(address1_1, size1_1));
        other.add(streamId2, index2_1, new SMREntryGarbageInfo(address2_1, size2_1));
        other.add(streamId2, index2_2, new SMREntryGarbageInfo(address2_2, size2_2));

        Assert.assertEquals(other, gcInfo);
    }

    @Test
    public void testMerge() {
        MultiObjectSMREntryGarbageInfo gcInfo = new MultiObjectSMREntryGarbageInfo();
        setUpInstance(gcInfo);

        MultiObjectSMREntryGarbageInfo other = new MultiObjectSMREntryGarbageInfo();

        long address1_2 = 3L;
        int size1_2 = 100;
        int index1_2 = 3;
        other.add(streamId1, index1_1, new SMREntryGarbageInfo(address1_1, size1_1));
        other.add(streamId1, index1_2, new SMREntryGarbageInfo(address1_2, size1_2));

        other.add(streamId2, index2_1, new SMREntryGarbageInfo(address2_1, size2_1));

        UUID streamId3 = UUID.randomUUID();
        long address3_1 = 5L;
        int size3_1 = 100;
        int index3_1 = 0;
        other.add(streamId3, index3_1, new SMREntryGarbageInfo(address3_1, size3_1));

        MultiObjectSMREntryGarbageInfo deduplicatedGCInfo = (MultiObjectSMREntryGarbageInfo) gcInfo.merge(other);

        MultiObjectSMREntryGarbageInfo expectedGCInfoAfterMerge = new MultiObjectSMREntryGarbageInfo();
        expectedGCInfoAfterMerge.add(streamId1, index1_1, new SMREntryGarbageInfo(address1_1, size1_1));
        expectedGCInfoAfterMerge.add(streamId1, index1_2, new SMREntryGarbageInfo(address1_2, size1_2));
        expectedGCInfoAfterMerge.add(streamId2, index2_1, new SMREntryGarbageInfo(address2_1, size2_1));
        expectedGCInfoAfterMerge.add(streamId2, index2_2, new SMREntryGarbageInfo(address2_2, size2_2));
        expectedGCInfoAfterMerge.add(streamId3, index3_1, new SMREntryGarbageInfo(address3_1, size3_1));
        Assert.assertEquals(expectedGCInfoAfterMerge, gcInfo);

        MultiObjectSMREntryGarbageInfo expectedDeduplicatedGCInfo = new MultiObjectSMREntryGarbageInfo();
        expectedDeduplicatedGCInfo.add(streamId1, index1_2, new SMREntryGarbageInfo(address1_2, size1_2));
        expectedDeduplicatedGCInfo.add(streamId3, index3_1, new SMREntryGarbageInfo(address3_1, size3_1));
        Assert.assertEquals(expectedDeduplicatedGCInfo, deduplicatedGCInfo);
    }

}

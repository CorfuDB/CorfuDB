/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.corfudb.infrastructure.log;


import io.netty.buffer.ByteBuf;

import java.io.File;

import io.netty.buffer.Unpooled;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.DataOutrankedException;
import org.corfudb.runtime.exceptions.ValueAdoptedException;
import org.corfudb.util.serializer.Serializers;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Created by Konstantin Spirov on 3/15/2017.
 */
public class StreamLogWithRankedAddressSpaceTest extends AbstractCorfuTest {

    private static final int RECORDS_TO_WRITE = 10;
    @Test
    @Ignore // compact on ranked address space not activated yet
    public void testCompact() throws Exception {
        StreamLogFiles log = new StreamLogFiles(getContext(), false);

        long address = 0;
        for (long x = 0; x < RECORDS_TO_WRITE; x++) {
            writeToLog(log, address, DataType.DATA, "Payload", x);
        }
        LogData value1 = log.read(address);
        long size1 = new File(log.getSegmentHandleForAddress(address).getFileName()).length();
        log.compact();
        LogData value2 = log.read(address);
        long size2 = new File(log.getSegmentHandleForAddress(address).getFileName()).length();
        assertEquals(value1.getRank(), value2.getRank());
        assertNotEquals(size2, size1);
    }

    @Test
    public void testHigherRank() {
        StreamLogFiles log = new StreamLogFiles(getContext(), false);
        long address = 0;
        writeToLog(log, address, DataType.DATA, "v-1", 1);
        LogData value1 = log.read(address);
        assertTrue(new String(value1.getData()).contains("v-1"));
        writeToLog(log, address, DataType.DATA, "v-2", 2);
        LogData value2 = log.read(address);
        assertTrue(new String(value2.getData()).contains("v-2"));
        log.close();
    }

    @Test
    public void testLowerRank() {
        StreamLogFiles log = new StreamLogFiles(getContext(), false);
        long address = 0;
        writeToLog(log, address, DataType.DATA, "v-1", 2);
        LogData value1 = log.read(address);
        assertTrue(new String(value1.getData()).contains("v-1"));
        try {
            writeToLog(log, address, DataType.DATA, "v-2", 1);
            fail();
        } catch (DataOutrankedException e) {
            // expected
        }
        LogData value2 = log.read(address);
        assertTrue(new String(value2.getData()).contains("v-1"));
        log.close();
    }

    @Test
    public void testHigherRankAgainstProposal() {
        StreamLogFiles log = new StreamLogFiles(getContext(), false);
        long address = 0;
        writeToLog(log, address, DataType.RANK_ONLY, "v-1", 1);
        LogData value1 = log.read(address);
        assertTrue(new String(value1.getData()).contains("v-1"));
        writeToLog(log, address, DataType.DATA, "v-2", 2);
        LogData value2 = log.read(address);
        assertTrue(new String(value2.getData()).contains("v-2"));
        log.close();
    }

    @Test
    public void testLowerRankAgainstProposal() {
        StreamLogFiles log = new StreamLogFiles(getContext(), false);
        long address = 0;
        writeToLog(log, address, DataType.RANK_ONLY, "v-1", 2);
        LogData value1 = log.read(address);
        assertTrue(new String(value1.getData()).contains("v-1"));
        try {
            writeToLog(log, address, DataType.DATA, "v-2", 1);
            fail();
        } catch (DataOutrankedException e) {
            // expected
        }
        LogData value2 = log.read(address);
        assertTrue(new String(value2.getData()).contains("v-1"));
        log.close();
    }

    @Test
    public void testProposalWithHigherRankAgainstData() {
        StreamLogFiles log = new StreamLogFiles(getContext(), false);
        long address = 0;
        writeToLog(log, address, DataType.DATA, "v-1", 1);
        LogData value1 = log.read(address);
        assertTrue(new String(value1.getData()).contains("v-1"));
        try {
            writeToLog(log, address, DataType.RANK_ONLY, "v-2", 2);
            fail();
        } catch (ValueAdoptedException e) {
            LogData logData = e.getReadResponse().getAddresses().get(0l);
            assertTrue(new String(logData.getData()).contains("v-1"));
        }
        LogData value2 = log.read(address);
        assertTrue(new String(value2.getData()).contains("v-1"));
        log.close();
    }


    @Test
    public void testProposalWithLowerRankAgainstData() {
        StreamLogFiles log = new StreamLogFiles(getContext(), false);
        long address = 0;
        writeToLog(log, address, DataType.DATA, "v-1", 2);
        LogData value1 = log.read(address);
        assertTrue(new String(value1.getData()).contains("v-1"));
        try {
            writeToLog(log, address, DataType.RANK_ONLY, "v-2", 1);
            fail();
        } catch (DataOutrankedException e) {
            // expected
        }
        LogData value2 = log.read(address);
        assertTrue(new String(value2.getData()).contains("v-1"));
        log.close();
    }


    @Test
    public void testProposalsHigherRank() {
        StreamLogFiles log = new StreamLogFiles(getContext(), false);
        long address = 0;
        writeToLog(log, address, DataType.RANK_ONLY, "v-1", 1);
        LogData value1 = log.read(address);
        assertTrue(new String(value1.getData()).contains("v-1"));
        writeToLog(log, address, DataType.RANK_ONLY, "v-2", 2);
        LogData value2 = log.read(address);
        assertTrue(new String(value2.getData()).contains("v-2"));
        log.close();
    }

    @Test
    public void testProposalsLowerRank() {
        StreamLogFiles log = new StreamLogFiles(getContext(), false);
        long address = 0;
        writeToLog(log, address, DataType.RANK_ONLY, "v-1", 2);
        LogData value1 = log.read(address);
        assertTrue(new String(value1.getData()).contains("v-1"));
        try {
            writeToLog(log, address, DataType.RANK_ONLY, "v-2", 1);
            fail();
        } catch (DataOutrankedException e) {
            // expected
        }
        LogData value2 = log.read(address);
        assertTrue(new String(value2.getData()).contains("v-1"));
        log.close();
    }

    @Test
    public void checkProposalIsIdempotent() {
        StreamLogFiles log = new StreamLogFiles(getContext(), false);
        long address = 0;
        IMetadata.DataRank sameRank = new IMetadata.DataRank(1);
        writeToLog(log, address, DataType.DATA, "v-1", sameRank);
        LogData value1 = log.read(address);
        assertTrue(new String(value1.getData()).contains("v-1"));
        writeToLog(log, address, DataType.DATA, "v-1", sameRank);
        LogData value2 = log.read(address);
        assertTrue(new String(value2.getData()).contains("v-1"));
    }

    private void writeToLog(StreamLog log, long address, DataType dataType, String payload, long rank) {
        this.writeToLog(log, address, dataType, payload, new IMetadata.DataRank(rank));
    }

    private void writeToLog(StreamLog log, long address, DataType dataType, String payload, IMetadata.DataRank rank) {
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = payload.getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        LogData data = new LogData(dataType, b);
        data.setRank(rank);
        log.append(address, data);
    }

    private String getDirPath() {
        return PARAMETERS.TEST_TEMP_DIR + File.separator;
    }

    private ServerContext getContext() {
        String path = getDirPath();
        return new ServerContextBuilder()
                .setLogPath(path)
                .setMemory(false)
                .build();
    }
}

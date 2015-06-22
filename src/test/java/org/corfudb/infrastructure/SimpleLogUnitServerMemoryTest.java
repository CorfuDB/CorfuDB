package org.corfudb.infrastructure;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.corfudb.infrastructure.thrift.ErrorCode;
import org.corfudb.infrastructure.thrift.ExtntMarkType;
import org.corfudb.infrastructure.thrift.ExtntWrap;
import org.corfudb.infrastructure.thrift.UnitServerHdr;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

public class SimpleLogUnitServerMemoryTest {

    private static byte[] getTestPayload(int size)
    {
        byte[] test = new byte[size];
        for (int i = 0; i < size; i++)
        {
            test[i] = (byte)(i % 255);
        }
        return test;
    }

    @Test
    public void checkIfLogUnitIsWritable() throws Exception
    {
        byte[] test = getTestPayload(4096);
        SimpleLogUnitServer slus = new SimpleLogUnitServer();
        slus.reset();
        for (int i = 0; i < 100; i++)
        {
            ArrayList<Integer> epochlist = new ArrayList<Integer>();
            epochlist.add(0);
            ArrayList<ByteBuffer> byteList = new ArrayList<ByteBuffer>();
            byteList.add(ByteBuffer.wrap(test));
            ErrorCode ec = slus.write(new UnitServerHdr(epochlist, i), byteList, ExtntMarkType.EX_FILLED);
            assertEquals(ec, ErrorCode.OK);
        }
    }

    @Test
    public void checkIfLogUnitIsWriteOnce() throws Exception
    {
        byte[] test = getTestPayload(4096);
        ArrayList<Integer> epochlist = new ArrayList<Integer>();
        epochlist.add(0);
        ArrayList<ByteBuffer> byteList = new ArrayList<ByteBuffer>();
        byteList.add(ByteBuffer.wrap(test));

        SimpleLogUnitServer slus = new SimpleLogUnitServer();
        slus.reset();
        for (int i = 0; i < 100; i++)
        {
            ErrorCode ec = slus.write(new UnitServerHdr(epochlist, i), byteList, ExtntMarkType.EX_FILLED);
            assertEquals(ec, ErrorCode.OK);
        }

        ErrorCode ec = slus.write(new UnitServerHdr(epochlist, 42), byteList, ExtntMarkType.EX_FILLED);
        assertEquals(ec, ErrorCode.ERR_OVERWRITE);
    }

    @Test
    public void checkIfLogIsReadable() throws Exception
    {
        byte[] test = getTestPayload(4096);
        ArrayList<Integer> epochlist = new ArrayList<Integer>();
        epochlist.add(0);
        ArrayList<ByteBuffer> byteList = new ArrayList<ByteBuffer>();
        byteList.add(ByteBuffer.wrap(test));

        SimpleLogUnitServer slus = new SimpleLogUnitServer();
        slus.reset();
        for (int i = 0; i < 100; i++)
        {
            ErrorCode ec = slus.write(new UnitServerHdr(epochlist, i), byteList, ExtntMarkType.EX_FILLED);
            assertEquals(ec, ErrorCode.OK);
        }

        ExtntWrap ew = slus.read(new UnitServerHdr(epochlist, 42));
        byte[] data = new byte[ew.getCtnt().get(0).limit()];
        ew.getCtnt().get(0).position(0);
        ew.getCtnt().get(0).get(data);
        assertArrayEquals(data, test);
    }

    @Test
    public void checkIfEmptyAddressesAreUnwritten() throws Exception
    {
        byte[] test = getTestPayload(4096);
        ArrayList<Integer> epochlist = new ArrayList<Integer>();
        epochlist.add(0);
        ArrayList<ByteBuffer> byteList = new ArrayList<ByteBuffer>();
        byteList.add(ByteBuffer.wrap(test));

        SimpleLogUnitServer slus = new SimpleLogUnitServer();
        slus.reset();
        for (int i = 0; i < 100; i++)
        {
            ErrorCode ec = slus.write(new UnitServerHdr(epochlist, i), byteList, ExtntMarkType.EX_FILLED);
            assertEquals(ec, ErrorCode.OK);
        }

        ExtntWrap ew = slus.read(new UnitServerHdr(epochlist, 101));
        assertEquals(ew.getErr(), ErrorCode.ERR_UNWRITTEN);
    }

    @Test
    public void basicMetadataTest() throws Exception {
        byte[] test = getTestPayload(4096);
        ArrayList<Integer> epochlist = new ArrayList<Integer>();
        epochlist.add(0);
        ArrayList<ByteBuffer> byteList = new ArrayList<ByteBuffer>();
        byteList.add(ByteBuffer.wrap(test));

        // Make sure no metadata in empty spot
        SimpleLogUnitServer slus = new SimpleLogUnitServer();
        slus.reset();
        ErrorCode ec = slus.setmetaNext(new UnitServerHdr(epochlist, 1), 1234L);
        assertEquals(ec, ErrorCode.ERR_UNWRITTEN);

        ExtntWrap ew = slus.readmeta(new UnitServerHdr(epochlist, 1));
        assertEquals(ew.getErr(), ErrorCode.ERR_UNWRITTEN);

        // Now there shuld be metadata
        ec = slus.write(new UnitServerHdr(epochlist, 1), byteList, ExtntMarkType.EX_FILLED);
        assertEquals(ec, ErrorCode.OK);
        ec = slus.setmetaNext(new UnitServerHdr(epochlist, 1), 1234L);
        assertEquals(ec, ErrorCode.OK);

        ew = slus.readmeta(new UnitServerHdr(epochlist, 1));
        assertEquals(ew.getErr(), ErrorCode.OK);
        assertEquals(ew.getInf().getNextOff(), 1234L);
        assertTrue(!ew.getInf().isTxDec());

        // Test TxDec
        ec = slus.setmetaTxDec(new UnitServerHdr(epochlist, 1), true);
        assertEquals(ec, ErrorCode.OK);
        ew = slus.readmeta(new UnitServerHdr(epochlist, 1));
        assertEquals(ew.getErr(), ErrorCode.OK);
        assertEquals(ew.getInf().getNextOff(), 1234L);
        assertTrue(ew.getInf().isTxDec());
    }
}

package org.corfudb.infrastructure;

import org.corfudb.infrastructure.thrift.*;
import org.corfudb.util.Utils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.*;

public class SimpleLogUnitServerMemoryTest {

    private static org.corfudb.infrastructure.thrift.UUID uuid = Utils.toThriftUUID(UUID.randomUUID());

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
            ErrorCode ec = slus.write(new UnitServerHdr(epochlist, i, Collections.singleton(uuid)), byteList, ExtntMarkType.EX_FILLED).getCode();
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
            ErrorCode ec = slus.write(new UnitServerHdr(epochlist, i, Collections.singleton(uuid)), byteList, ExtntMarkType.EX_FILLED).getCode();
            assertEquals(ec, ErrorCode.OK);
        }

        ErrorCode ec = slus.write(new UnitServerHdr(epochlist, 42, Collections.singleton(uuid)), byteList, ExtntMarkType.EX_FILLED).getCode();
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
            ErrorCode ec = slus.write(new UnitServerHdr(epochlist, i, Collections.singleton(uuid)), byteList, ExtntMarkType.EX_FILLED).getCode();
            assertEquals(ec, ErrorCode.OK);
        }

        ExtntWrap ew = slus.read(new UnitServerHdr(epochlist, 42, Collections.singleton(uuid)));
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
            ErrorCode ec = slus.write(new UnitServerHdr(epochlist, i, Collections.singleton(uuid)), byteList, ExtntMarkType.EX_FILLED).getCode();
            assertEquals(ec, ErrorCode.OK);
        }

        ExtntWrap ew = slus.read(new UnitServerHdr(epochlist, 101, Collections.singleton(uuid)));
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
        ErrorCode ec = slus.setHintsNext(new UnitServerHdr(epochlist, 1, Collections.singleton(uuid)), 1234L);
        assertEquals(ec, ErrorCode.ERR_UNWRITTEN);

        Hints hint = slus.readHints(new UnitServerHdr(epochlist, 1, Collections.singleton(uuid)));
        assertEquals(hint.getErr(), ErrorCode.ERR_UNWRITTEN);

        // Now there shuld be metadata
        ec = slus.write(new UnitServerHdr(epochlist, 1, Collections.singleton(uuid)), byteList, ExtntMarkType.EX_FILLED).getCode();
        assertEquals(ec, ErrorCode.OK);
        ec = slus.setHintsNext(new UnitServerHdr(epochlist, 1, Collections.singleton(uuid)), 1234L);
        assertEquals(ec, ErrorCode.OK);

        hint = slus.readHints(new UnitServerHdr(epochlist, 1, Collections.singleton(uuid)));
        assertEquals(hint.getErr(), ErrorCode.OK);
        assertEquals(hint.getNextMap().get(uuid).longValue(), 1234L);
        assertTrue(!hint.isTxDec());

        // Test TxDec
        ec = slus.setHintsTxDec(new UnitServerHdr(epochlist, 1, Collections.singleton(uuid)), true);
        assertEquals(ec, ErrorCode.OK);
        hint = slus.readHints(new UnitServerHdr(epochlist, 1, Collections.singleton(uuid)));
        assertEquals(hint.getErr(), ErrorCode.OK);
        assertEquals(hint.getNextMap().get(uuid).longValue(), 1234L);
        assertTrue(hint.isTxDec());
    }
}

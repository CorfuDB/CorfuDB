package org.corfudb.infrastructure;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.corfudb.infrastructure.thrift.ErrorCode;
import org.corfudb.infrastructure.thrift.ExtntMarkType;
import org.corfudb.infrastructure.thrift.ExtntWrap;
import org.corfudb.infrastructure.thrift.UnitServerHdr;
import org.junit.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class SimpleLogUnitServerDiskTest {

    private static byte[] getTestPayload(int size)
    {
        byte[] test = new byte[size];
        for (int i = 0; i < size; i++)
        {
            test[i] = (byte)(i % 255);
        }
        return test;
    }    private static SimpleLogUnitServer slus = new SimpleLogUnitServer();

    private static String TESTFILE = "testFile";
    private static int PAGESIZE = 4096;

    private static byte[] test = getTestPayload(PAGESIZE);
    private static ArrayList<Integer> epochlist = new ArrayList<Integer>();
    private static ArrayList<ByteBuffer> byteList = new ArrayList<ByteBuffer>();

    @BeforeClass
    public static void setupServer() throws Exception {
        HashMap<String, Object> configMap = new HashMap<String, Object>();
        configMap.put("ramdisk", false);
        configMap.put("capacity", 1000);
        configMap.put("port", 0);
        configMap.put("pagesize", PAGESIZE);
        configMap.put("trim", -1);
        configMap.put("drive", TESTFILE);
        Thread t = new Thread(slus.getInstance(configMap));
        t.start();

        epochlist.add(0);
        byteList.add(ByteBuffer.wrap(test));

        // Wait for server thread to finish setting up
        boolean done = false;

        while (!done) {
            try {
                slus.write(new UnitServerHdr(epochlist, 0, Collections.singleton("fake stream")), byteList, ExtntMarkType.EX_FILLED);
                done = true;
            } catch (NullPointerException e) {}
        }

        // Write entries in for the tests
        for (int i = 1; i < 100; i++)
        {
            byteList.get(0).position(0);
            ErrorCode ec = slus.write(new UnitServerHdr(epochlist, i, Collections.singleton("fake stream")), byteList, ExtntMarkType.EX_FILLED);
            assertEquals(ec, ErrorCode.OK);
        }
    }

    @After
    public void tearDown() {
        File file = new File(TESTFILE);
        file.delete();
    }

    @Test
    public void checkIfLogUnitIsWriteOnce() throws Exception
    {
        ErrorCode ec = slus.write(new UnitServerHdr(epochlist, 42, Collections.singleton("fake stream")), byteList, ExtntMarkType.EX_FILLED);
        assertEquals(ErrorCode.ERR_OVERWRITE, ec);
    }


    @Test
    public void checkIfLogIsReadable() throws Exception
    {
        ExtntWrap ew = slus.read(new UnitServerHdr(epochlist, 1, Collections.singleton("fake stream")));
        byte[] data = new byte[ew.getCtnt().get(0).limit()];
        ew.getCtnt().get(0).position(0);
        ew.getCtnt().get(0).get(data);
        assertArrayEquals(test, data);
    }

    @Test
    public void checkIfEmptyAddressesAreUnwritten() throws Exception
    {
        ExtntWrap ew = slus.read(new UnitServerHdr(epochlist, 101, Collections.singleton("fake stream")));
        assertEquals(ew.getErr(), ErrorCode.ERR_UNWRITTEN);
    }
}

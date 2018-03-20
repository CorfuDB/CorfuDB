package org.corfudb.logReader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.corfudb.format.Types;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.log.StreamLogFiles;

import static org.junit.Assert.*;

import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.util.serializer.Serializers;
import org.docopt.DocoptExitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by kjames88 on 3/1/17.
 */

public class logReaderTest {
    public static String LOG_BASE_PATH = "/tmp/corfu-test";
    public static String LOG_PATH = LOG_BASE_PATH + "/log";

    private ServerContext getContext() {
        return new ServerContextBuilder()
            .setLogPath(LOG_BASE_PATH)
            .setNoVerify(false)
            .build();
    }

    @Before
    public void setUp() {
        File fDir = new File(LOG_PATH);
        fDir.mkdirs();
        StreamLogFiles logfile = new StreamLogFiles(getContext(), false);
        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
        Serializers.CORFU.serialize("Hello World".getBytes(), buf);
        LogData data = new LogData(DataType.DATA, buf);
        logfile.append(0, data);
        buf.clear();
        Serializers.CORFU.serialize("Happy Days".getBytes(), buf);
        data = new LogData(DataType.DATA, buf);
        logfile.append(1, data);
        buf.clear();
        Serializers.CORFU.serialize("Corfu test".getBytes(), buf);
        data = new LogData(DataType.DATA, buf);
        logfile.append(2, data);
        logfile.close();
    }
    @After
    public void tearDown() {
        File fDir = new File(LOG_PATH);
        String[] files = fDir.list();
        for (String f : files) {
            File f_del = new File(LOG_PATH + "/" + f);
            f_del.delete();
        }
        fDir.delete();
        fDir = new File(LOG_BASE_PATH);
        fDir.delete();
    }
    @Test(expected = DocoptExitException.class)
    public void TestArgumentsRequired() {
        logReader reader = new logReader();
        String[] args = {};
        boolean ret = reader.init(args);
        assertEquals(false, ret);
    }
    @Test
    public void TestRecordCount() {
        final int totalRecordCnt = 3;

        logReader reader = new logReader();
        String[] args = {"report", LOG_PATH + "/" + "0.log"};
        int cnt = reader.run(args);
        assertEquals(totalRecordCnt, cnt);
    }
    @Test
    public void TestDisplayOne() {
        logReader reader = new logReader();
        String[] args = {"display", "--from=1", "--to=1", LOG_PATH + "/" + "0.log"};
        reader.init(args);
        try {
            reader.openLogFile(0);
            LogEntryExtended e = reader.nextRecord();
            assertEquals(null, e);
            e = reader.nextRecord();
            assertNotEquals(null, e);
            e = reader.nextRecord();
            assertEquals(null, e);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }
    @Test
    public void TestDisplayAll() {
        logReader reader = new logReader();
        String[] args = {"display", "--from=0", "--to=2", LOG_PATH + "/" + "0.log"};
        reader.init(args);
        try {
            reader.openLogFile(0);
            LogEntryExtended e = reader.nextRecord();
            assertNotEquals(null, e);
            e = reader.nextRecord();
            assertNotEquals(null, e);
            e = reader.nextRecord();
            assertNotEquals(null, e);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }
    @Test
    public void TestEraseOne() {
        logReader reader = new logReader();
        String[] args = {"erase", "--from=1", "--to=1", LOG_PATH + "/" + "0.log"};
        reader.run(args);

        // Read back the new modified log file and confirm the expected change
        reader = new logReader();
        args = new String[]{"display", LOG_PATH + "/" + "0.log.modified"};
        reader.init(args);
        try {
            reader.openLogFile(0);
            LogEntryExtended e = reader.nextRecord();
            assertEquals(Types.DataType.DATA, e.getEntryBody().getDataType());
            e = reader.nextRecord();
            assertEquals(Types.DataType.HOLE, e.getEntryBody().getDataType());
            e = reader.nextRecord();
            assertEquals(Types.DataType.DATA, e.getEntryBody().getDataType());
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }
    @Test
    public void TestEraseTail() {
        logReader reader = new logReader();
        String[] args = {"erase", "--from=1", LOG_PATH + "/" + "0.log"};
        reader.run(args);

        // Read back the new modified log file and confirm the expected change
        reader = new logReader();
        args = new String[]{"display", LOG_PATH + "/" + "0.log.modified"};
        reader.init(args);
        try {
            reader.openLogFile(0);
            LogEntryExtended e = reader.nextRecord();
            assertEquals(Types.DataType.DATA, e.getEntryBody().getDataType());
            e = reader.nextRecord();
            assertEquals(Types.DataType.HOLE, e.getEntryBody().getDataType());
            e = reader.nextRecord();
            assertEquals(Types.DataType.HOLE, e.getEntryBody().getDataType());
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }
}
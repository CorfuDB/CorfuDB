package org.corfudb.logReader;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.corfudb.format.Types;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.log.LogAddress;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.util.serializer.Serializers;
import org.docopt.DocoptExitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Created by kjames88 on 3/1/17.
 */

public class logReaderTest {
    public static String LOG_BASE_PATH = "/tmp/corfu-test";
    public static String LOG_PATH = LOG_BASE_PATH + "/log";

    private ServerContext getContext() {
        Map<String,Object> configs = new HashMap();
        configs.put("--log-path", LOG_BASE_PATH);
        configs.put("--no-verify", false);
        configs .put("--cache-heap-ratio", "0.5");
        return new ServerContext(configs, null);
    }

    @Before
    public void setUp() {
        testUUID = UUID.randomUUID();
        File fDir = new File(LOG_PATH);
        fDir.mkdirs();
        StreamLogFiles logfile = new StreamLogFiles(getContext(), false);
        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
        Serializers.CORFU.serialize("Hello World".getBytes(), buf);
        LogData data = new LogData(DataType.DATA, buf);
        logfile.append(new LogAddress(new Long(0), testUUID), data);
        buf.clear();
        Serializers.CORFU.serialize("Happy Days".getBytes(), buf);
        data = new LogData(DataType.DATA, buf);
        logfile.append(new LogAddress(new Long(1), testUUID), data);
        buf.clear();
        Serializers.CORFU.serialize("Corfu test".getBytes(), buf);
        data = new LogData(DataType.DATA, buf);
        logfile.append(new LogAddress(new Long(2), testUUID), data);
        logfile.close();
    }

    public void corruptLog() {
        // First add an extra metadata at end of file
        String logFilePath = LOG_PATH + "/" + testUUID + "-0.log";
        try {
            RandomAccessFile file = new RandomAccessFile(logFilePath, "rw");
            file.seek(file.length());
            Types.Metadata md = Types.Metadata.newBuilder()
                    .setChecksum(0)
                    .setLength(16)  // size is arbitrary but cannot be 0 (default)
                    .build();

            byte[] buf = md.toByteArray();
            file.write(buf);
            file.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addLogRecord() {
        String logFilePath = LOG_PATH + "/" + testUUID + "-0.log";
        try {
            RandomAccessFile file = new RandomAccessFile(logFilePath, "rw");
            file.seek(file.length());
            ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
            Serializers.CORFU.serialize("Brave New World".getBytes(), buf);
            LogData data = new LogData(DataType.DATA, buf);
            Types.LogEntry leNew = Types.LogEntry.newBuilder()
                .setData(ByteString.copyFrom(data.getData()))
                .setGlobalAddress(0x4)
                .setDataType(Types.DataType.DATA)
                .addStreams(testUUID.toString())
                .setCommit(false)
                .build();
            byte [] leb = leNew.toByteArray();
            Types.Metadata mdNew = Types.Metadata.newBuilder()
                .setLength(leb.length)
                .setChecksum(StreamLogFiles.getChecksum(leb))
                .build();
            byte [] mdb = mdNew.toByteArray();
            // Delimiter
            file.writeShort(StreamLogFiles.RECORD_DELIMITER);
            // MetaData
            file.write(mdb);
            // LogEntry
            file.write(leb);
            file.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        String[] args = {"report", LOG_PATH + "/" + testUUID + "-0.log"};
        reader.init(args);
        reader.setShowOutput(false);
        int cnt = reader.readAll();
        assertEquals(totalRecordCnt, cnt);
    }
    @Test
    public void TestDisplayOne() {
        logReader reader = new logReader();
        String[] args = {"display", "--from=1", "--to=1", LOG_PATH + "/" + testUUID + "-0.log"};
        reader.init(args);
        reader.setShowOutput(false);
        try {
            reader.openLogFile();
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
        String[] args = {"display", "--from=0", "--to=2", LOG_PATH + "/" + testUUID + "-0.log"};
        reader.init(args);
        reader.setShowOutput(false);
        try {
            reader.openLogFile();
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
        String[] args = {"erase", "--from=1", "--to=1", LOG_PATH + "/" + testUUID + "-0.log"};
        reader.init(args);
        reader.setShowOutput(false);
        reader.readAll();

        // Read back the new modified log file and confirm the expected change
        reader = new logReader();
        args = new String[]{"display", LOG_PATH + "/" + testUUID + "-0.log.modified"};
        reader.init(args);
        reader.setShowOutput(false);
        try {
            reader.openLogFile();
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
        String[] args = {"erase", "--from=1", LOG_PATH + "/" + testUUID + "-0.log"};
        reader.init(args);
        reader.setShowOutput(false);
        reader.readAll();

        // Read back the new modified log file and confirm the expected change
        reader = new logReader();
        args = new String[]{"display", LOG_PATH + "/" + testUUID + "-0.log.modified"};
        reader.init(args);
        reader.setShowOutput(false);
        try {
            reader.openLogFile();
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

    @Test(expected=RuntimeException.class)
    public void TestResyncFails() {
        corruptLog();
        logReader reader = new logReader();
        String[] args = new String[]{"display", LOG_PATH + "/" + testUUID + "-0.log"};
        reader.init(args);
        reader.setShowOutput(false);
        reader.readAll();
    }

    @Test
    public void TestResyncSucceeds() {
        corruptLog();
        addLogRecord();
        logReader reader = new logReader();
        String[] args = new String[]{"display", LOG_PATH + "/" + testUUID + "-0.log"};
        reader.init(args);
        reader.setShowOutput(false);
        reader.readAll();
    }

    private UUID testUUID;
}
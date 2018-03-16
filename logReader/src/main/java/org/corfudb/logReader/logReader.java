package org.corfudb.logReader;

import com.google.protobuf.ByteString;
import org.corfudb.format.Types;
import org.corfudb.format.Types.DataType;
import org.corfudb.format.Types.LogEntry;
import org.corfudb.format.Types.LogHeader;
import org.corfudb.format.Types.Metadata;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.docopt.Docopt;
import org.docopt.DocoptExitException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.UUID;

public class logReader {
    private static int metadataSize;
    private static final int NON_ZERO = 16;
    private static final String USAGE =
            "Usage:\n"
                    + "\tlogReader report <log_file>\n"
                    + "\tlogReader display <log_file> [--from=<address> --to=<address> --show_binary]\n"
                    + "\tlogReader erase <log_file> [--from=<address> --to=<address>]\n"
                    + "\n"
                    + "Options:\n"
                    + "\t--show_binary      display binary data\n"
                    + "\t--from=<address>   starting address (default=0)\n"
                    + "\t--to=<address>     final address (default=unlimited)\n";

    private Operation op = null;
    private FileInputStream fileStreamIn = null;
    private FileOutputStream fileStreamOut = null;
    private FileChannel fileChannelIn = null;
    private FileChannel fileChannelOut = null;
    private int recordCnt = 0;
    private long remSize = 0;

    public static void main(final String[] args) {
        logReader reader = new logReader();
        reader.run(args);
        reader.cleanUp();
    }

    public logReader() {
        fileStreamIn = null;
        fileStreamOut = null;
    }

    @SuppressWarnings("checkstyle:printLine") // Utility
    public final int run(final String[] args) {
        try {
            boolean ret = init(args);
        } catch (DocoptExitException e) {
            System.out.println(USAGE);
            System.exit(1);
        }
        return readAll();
    }

    @SuppressWarnings({"checkstyle:printLine", "checkstyle:print"}) // Utility
    public final boolean init(final String[] args) {
        Docopt parser = new Docopt(USAGE);
        parser.withExit(false);
        Map<String, Object> opts = parser.parse(args);
        op = new Operation(Operation.OperationType.REPORT);
        String logFileName = new String();
        boolean useOutputFile = false;
        if (opts.get("<log_file>") != null) {
            logFileName = (String) opts.get("<log_file>");
            System.out.println("Log file: " + logFileName);
        } else {
            System.out.print(USAGE);
            return false;
        }

        int startAddr = 0;
        int finalAddr = -1;
        if (opts.get("--from") != null) {
            startAddr = Integer.parseInt((String) opts.get("--from"));
        }
        if (opts.get("--to") != null) {
            finalAddr = Integer.parseInt((String) opts.get("--to"));
        }
        if ((Boolean) opts.get("display")) {
            Boolean showBinary = (Boolean) opts.get("--show_binary");
            System.out.format("display from %d to %d show_binary=%s\n", startAddr, finalAddr, showBinary.toString());
            if (showBinary) {
                op = new Operation(Operation.OperationType.DISPLAY, startAddr, finalAddr);
            } else {
                op = new Operation(Operation.OperationType.DISPLAY_ALL, startAddr, finalAddr);
            }
        } else if ((Boolean) opts.get("erase")) {
            System.out.format("erase from %d to %d\n", startAddr, finalAddr);
            op = new Operation(Operation.OperationType.ERASE_RANGE, startAddr, finalAddr);
            useOutputFile = true;
        }

        Metadata md = Metadata.newBuilder()
                .setChecksum(NON_ZERO)
                .setLength(NON_ZERO)  // size is arbitrary but cannot be 0 (default)
                .build();
        metadataSize = md.getSerializedSize();

        File fIn = new File(logFileName);
        File fOut = useOutputFile ? new File(logFileName + ".modified") : null;
        if (fIn.canRead()) {
            try {
                fileStreamIn = new FileInputStream(fIn);
                fileChannelIn = fileStreamIn.getChannel();
                if (useOutputFile) {
                    fileStreamOut = new FileOutputStream(fOut);
                    fileChannelOut = fileStreamOut.getChannel();
                }
            } catch (IOException e) {
                throw new UnrecoverableCorfuError(e);
            }
            return true;
        }
        return false;
    }

    final int readAll() {
        int recordCnt = 0;
        try {
            recordCnt = processLogFile();
        } catch (IOException e) {
            throw new UnrecoverableCorfuError(e);
        }
        return recordCnt;
    }

    final void cleanUp() {
        try {
            if (fileStreamIn != null) {
                fileStreamIn.close();
                fileStreamIn = null;
            }
            if (fileStreamOut != null) {
                fileStreamOut.close();
                fileStreamOut = null;
            }
        } catch (IOException e) {
            throw new UnrecoverableCorfuError(e);
        }
    }

    final LogEntry buildHoleLogEntry(final LogEntry entry) {
        LogEntry.Builder leNew = LogEntry.newBuilder();
        leNew.mergeFrom(entry);
        leNew.clearData();
        leNew.setDataType(DataType.HOLE);
        return leNew.build();
    }

    final void writeBuffer(final LogEntry le, final FileChannel fcOut) throws IOException {
        byte[] b1 = le.toByteArray();
        int cksum = StreamLogFiles.getChecksum(b1);
        ByteBuffer recordBuffer = ByteBuffer.allocate(b1.length);
        recordBuffer.put(b1);
        recordBuffer.flip();
        Metadata mdNew = Metadata.newBuilder()
                .setLength(b1.length)
                .setChecksum(cksum)
                .build();
        byte[] b2 = mdNew.toByteArray();
        ByteBuffer mdBuff = ByteBuffer.allocate(metadataSize);
        mdBuff.put(b2);
        mdBuff.flip();
        writeRecord(fcOut, mdBuff, recordBuffer);
    }

    final void writeRecord(final FileChannel fcOut,
                           final ByteBuffer mdBuff,
                           final ByteBuffer recordBuffer) throws IOException {
        assert (fcOut != null);
        ByteBuffer commaBuffer = ByteBuffer.allocate(2);
        commaBuffer.putShort(StreamLogFiles.RECORD_DELIMITER);
        commaBuffer.flip();
        fcOut.write(commaBuffer);
        fcOut.write(mdBuff);
        fcOut.write(recordBuffer);
    }

    @SuppressWarnings({"checkstyle:printLine", "checkstyle:print"}) // Utility
    public final void printLogEntry(final LogEntry entry, final boolean showBinary) {
        System.out.format("Global address: %d\n", entry.getGlobalAddress());
        System.out.format("Log Entry streams (%d):  ", entry.getStreamsCount());
        for (int i = 0; i < entry.getStreamsCount(); i++) {
            System.out.print(entry.getStreams(i) + " ");
        }
        System.out.format("\n");
        String bstr = new String();
        if (showBinary) {
            ByteString dbuff = entry.getData();
            for (int i = 0; i < dbuff.size(); i++) {
                byte c = dbuff.byteAt(i);
                if (Character.isLetterOrDigit(c)) {
                    bstr += (char) c;
                } else {
                    bstr += String.format("\\x%02x", c);
                }
            }
        }
        DataType dt = entry.getDataType();
        switch (dt) {
            case DATA:
                System.out.println("DataType: DATA");
                break;
            case EMPTY:
                System.out.println("DataType: EMPTY");
                break;
            case HOLE:
                System.out.println("DataType: HOLE");
                break;
            case TRIMMED:
                System.out.println("DataType: TRIMMED");
                break;
            default:
                System.out.printf("UNKNOWN DataType %s\n", dt);
                break;
        }
        if (showBinary) {
            System.out.format("Data:\n%s\n", bstr);
        }
        Types.DataRank dr = entry.getRank();
        System.out.format("Rank: %d, UUID: %x %x\n",
                dr.hasRank() ? dr.getRank() : 0L,
                dr.hasUuidMostSignificant() ? dr.getUuidMostSignificant() : 0L,
                dr.hasUuidLeastSignificant() ? dr.getUuidLeastSignificant() : 0L);
        if (entry.hasCheckpointEntryType()) {
            System.out.format("Checkpoint type: %s, ID %s\n",
                    entry.getCheckpointEntryType(),
                    new UUID(entry.hasCheckpointIdMostSignificant()
                             ? entry.getCheckpointIdMostSignificant()
                             : 0L,
                             entry.hasCheckpointIdLeastSignificant()
                             ? entry.getCheckpointIdLeastSignificant()
                             : 0L));
        }
    }

    // Read and conditionally replace a record
    //   - replace record in case of ERASE or ERASE_TAIL
    //   - returns same or modified record
    final LogEntry processRecordBody(final ByteBuffer recordBuffer) throws IOException {
        LogEntry le = LogEntry.parseFrom(recordBuffer.array());
        if (op.getOpType() == Operation.OperationType.ERASE_RANGE) {
            if (op.isInRange(le.getGlobalAddress())) {
                LogEntry entry = buildHoleLogEntry(le);
                return entry;
            }
        }
        return le;
    }

    final logHeader processHeader() throws IOException {
        fileChannelIn.position(0);
        ByteBuffer mdBuffer = ByteBuffer.allocate(metadataSize);
        int r = fileChannelIn.read(mdBuffer);
        mdBuffer.flip();
        if (fileChannelOut != null) {
            fileChannelOut.write(mdBuffer);
        }
        if (r > 0) {
            logHeader header = new logHeader();
            Metadata md = Metadata.parseFrom(mdBuffer.array());
            int logHeaderSize = md.getLength();
            header.setChecksum(md.getChecksum());
            header.setLength(md.getLength());
            ByteBuffer lhBuffer = ByteBuffer.allocate(logHeaderSize);
            r = fileChannelIn.read(lhBuffer);
            lhBuffer.flip();
            if (fileChannelOut != null) {
                fileChannelOut.write(lhBuffer);
            }
            if (r > 0) {
                LogHeader lh = LogHeader.parseFrom(lhBuffer.array());
                header.setVersion(lh.getVersion());
                header.setVerifyChecksum(lh.getVerifyChecksum());
            }
            return header;
        }
        return new logHeader();
    }

    @SuppressWarnings("checkstyle:printLine")
    final LogEntryExtended processRecord() throws IOException {
        ByteBuffer commaBuffer = ByteBuffer.allocate(2);
        int bytesRead = fileChannelIn.read(commaBuffer);
        commaBuffer.flip();
        Short delim = commaBuffer.getShort();
        commaBuffer.flip();
        if (delim != StreamLogFiles.RECORD_DELIMITER) {
            System.out.println("Incorrect delimiter");
        }
        ByteBuffer mdBuffer = ByteBuffer.allocate(metadataSize);
        bytesRead += fileChannelIn.read(mdBuffer);
        mdBuffer.flip();
        Metadata md = Metadata.parseFrom(mdBuffer.array());
        ByteBuffer recordBuffer = ByteBuffer.allocate(md.getLength());
        bytesRead += fileChannelIn.read(recordBuffer);
        recordBuffer.flip();
        int cksum = StreamLogFiles.getChecksum(recordBuffer.array());
        if (cksum != md.getChecksum()) {
            System.out.println("Checksum ERROR");
        }
        LogEntry leNew = processRecordBody(recordBuffer);
        return new LogEntryExtended(leNew, bytesRead, cksum);
    }

    @SuppressWarnings("checkstyle:printLine")
    final void openLogFile(final int display) throws IOException {
        logHeader hdr = processHeader();
        if (display > 0) {
            System.out.println("file size " + fileChannelIn.size());
            System.out.println("checksum " + String.format("%08x", hdr.getChecksum()));
            System.out.println("length " + Integer.toString(hdr.getLength()));
            System.out.println("version " + Integer.toString(hdr.getVersion()));
            System.out.println("verify " + Boolean.toString(hdr.isVerifyChecksum()));
        }
        remSize = fileChannelIn.size() - fileChannelIn.position();  // if size == position then file pointer is off the end
    }

    final LogEntryExtended nextRecord() throws IOException {
        LogEntryExtended leNew = processRecord();
        long addr = leNew.getEntryBody().getGlobalAddress();
        boolean display = (op.getOpType() == Operation.OperationType.DISPLAY
            || op.getOpType() == Operation.OperationType.DISPLAY_ALL) && op.isInRange(addr);
        if (display) {
            System.out.format("Record length %d checksum %08x\n", leNew.getBytesLength(), leNew.getChecksum());
            printLogEntry(leNew.getEntryBody(), op.getOpType() == Operation.OperationType.DISPLAY_ALL);
        }
        if (fileChannelOut != null) {
            writeBuffer(leNew.getEntryBody(), fileChannelOut);
        }
        recordCnt++;
        remSize -= leNew.getBytesLength();
        if (display)
            return leNew;
        return null;
    }

    @SuppressWarnings("checkstyle:printLine")
    final int processLogFile() throws IOException {
        int display = op.getOpType() == Operation.OperationType.DISPLAY ? 1
                : op.getOpType() == Operation.OperationType.DISPLAY_ALL ? 2 : 0;

        System.out.format("processLogFile display %d output %d\n", display,
                fileChannelOut != null ? 1 : 0);

        // Metadata
        //   LogHeader
        // Metadata
        //   LogEntry
        // ...
        openLogFile(display);
        while (remSize > 0) {
            nextRecord();
        }
        // REPORT
        System.out.println("Read " + Integer.toString(recordCnt) + " log entries");
        return recordCnt;
    }


}

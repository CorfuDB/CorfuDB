package org.corfudb.infrastructure;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import com.google.common.collect.HashBasedTable;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.IOLatencyDetector;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import static java.lang.Thread.sleep;

//numthread that reading file
//numthread that writing to a file
//create N files with size
//each thread doing read write file operations

@Slf4j
public class IOLatencyDetectorFileTest {
    static int BLOCK_SIZE = (4 << 10);

    static class Args {
        @Parameter(names = {"-h", "--help"}, description = "Help message", help = true)
        boolean help;

        @Parameter(names = {"--num-reads"}, description = "Number of threads will do file read", required = true)
        int numReads;

        @Parameter(names = {"--num-writes"}, description = "Number of threads will do file write", required = true)
        int numWrites;

        @Parameter(names = {"--file-size"}, description = "The file size", required = true)
        long fileSize;

        @Parameter(names = {"--num-repeats"}, description = "The number to repeat read/write the same file", required = true)
        int numRepeat;

        @Parameter(names = {"--num-polls"}, description = "The number to poll the iolatencydetector", required = true)
        long numPolls;

        @Parameter(names = {"--num-wlfiles"}, description = "The number files to generate workload", required = true)
        int numWLFiles;

        @Parameter(names = {"--file-prefix"}, description = "The file prefix", required = true)
        String filePrefix;
    }

    static void createFlies(int numFiles, long fileSize, String filePrefix) throws IOException {
        for (int i = 0; i < numFiles; i++) {
            String file = filePrefix + Integer.toString (i);
            RandomAccessFile raf = new RandomAccessFile (file, "rw");
            raf.setLength (fileSize);
            log.info("creatfile " + file + " size " + fileSize );
            raf.close ();
        }
    }

    static class FileCreator implements  Runnable {
        private Thread t;
        private String threadName;
        private int numFiles;
        private long fileSize;

        public FileCreator(int numFiles, long fileSize, String threadName) {
            this.threadName = threadName;
            this.numFiles = numFiles;
            this.fileSize = fileSize;
        }

        public void start () {
            System.out.println ("Starting " +  threadName);
            if (t == null) {
                t = new Thread (this, threadName);
                t.start();
            }
        }

        @Override
        public void run() {
            try {
                createFlies (numFiles, fileSize, threadName);
            } catch (IOException e) {
                e.printStackTrace ();
            }
        }
    }

    static class WorkLoadGenerator {
        static public void start(int numThreads, long fileSize, String fileName) {
            for (int i = 0; i < numThreads; i++) {
                FileCreator fcreator = new FileCreator (1, fileSize, fileName + "workload" + Integer
                        .toString (i));
                fcreator.start();
            }
        }
    }

    static class ReaderWriter implements Runnable {
        private Thread t;
        private String fileName;
        int numRepeat = 0;
        boolean reader;
        long fileSize;

        void readOneFile(String file, long n) throws IOException {
            File aFile = new File (file);
            FileInputStream inFile = new FileInputStream (aFile);
            FileChannel inChannel = inFile.getChannel ();
            ByteBuffer buf = ByteBuffer.allocate (BLOCK_SIZE);

            while (n-- > 0) {
                int res = 1;
                while (res > 0) {
                    res = IOLatencyDetector.read (inChannel, buf, false, 0);
                    buf.clear ();
                }
                System.out.println("Readfile  " + fileName + " time " + System.nanoTime ()/1000 + " round " + n);
                inChannel.position (0);
            }
            inFile.close ();
        }

        void writeOneFile(String file, long n, long fileSize) throws IOException {
            File aFile = new File (file);
            FileOutputStream outFile = new FileOutputStream (aFile);
            FileChannel outChannel = outFile.getChannel();
            ByteBuffer buf = ByteBuffer.allocate (BLOCK_SIZE);

            while (n-- > 0) {
                int i = 0;
                while (i < (fileSize / BLOCK_SIZE)) {
                    Arrays.fill(buf.array (), (byte)0);
                    IOLatencyDetector.write(outChannel, buf, false, 0);
                    //if (i % 20 == 0)
                        IOLatencyDetector.force(outChannel,true);
                    buf.clear ();
                    Arrays.fill(buf.array (), (byte)0);
                    i++;
                }
                System.out.println("Writefile: " + fileName + " time " + System.nanoTime ()/1000 + " round " + n);
                outChannel.position (0);
            }
            outFile.close ();
        }

        ReaderWriter(String name, int numRepeat, boolean reader, long fileSize) {
            this.fileName = name;
            this.numRepeat = numRepeat;
            this.reader = reader;
            this.fileSize = fileSize;
            System.out.println("Creating ReaderWriter " + fileName + " numRepeat " + numRepeat + " reader " + reader);
        }


        public void start () {
            System.out.println ("Starting " +  fileName);
            if (t == null) {
                t = new Thread (this, fileName);
                t.start();
            }
        }

        @Override
        public void run() {
            try {
                if (reader) {
                    readOneFile (fileName, numRepeat);
                } else {
                    writeOneFile (fileName, numRepeat, fileSize);
                }
            } catch (IOException e) {
                e.printStackTrace ();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        IOLatencyDetector.setupIOLatencyDetector (10, 1);

        Args cmdArgs = new Args ();
        JCommander jc = JCommander.newBuilder ()
                .addObject (cmdArgs)
                .build ();
        jc.parse (args);

        if (cmdArgs.help) {
            jc.usage ();
            System.exit (0);
        }

        int numReads = cmdArgs.numReads;
        int numWrites = cmdArgs.numWrites;
        int numRepeats = cmdArgs.numRepeat;
        int numWLFiles = cmdArgs.numWLFiles;
        long fileSize = cmdArgs.fileSize;
        long numPolls = cmdArgs.numPolls;
        String filePrefix = cmdArgs.filePrefix;

        IOLatencyDetector.setupIOLatencyDetector (1000, 5);
        createFlies(numReads + numWrites, fileSize, filePrefix);

        int i;
        for (i = 0; i < numReads; i++) {
            String fileName = filePrefix + Integer.toString (i);
            ReaderWriter rw = new ReaderWriter (fileName, numRepeats, true, fileSize);
            rw.start ();
        }

        for (; i < numReads + numWrites; i++) {
            String fileName = filePrefix + Integer.toString (i);
            ReaderWriter rw = new ReaderWriter (fileName, numRepeats, false, fileSize);
            rw.start ();
        }

        for(long j = 0; j < numPolls; j++) {
            sleep (500);
            if (numPolls % 1000 == 0) {
                WorkLoadGenerator.start (numWLFiles, (4 << 20), filePrefix);
                sleep(10);
            }
            if (IOLatencyDetector.reportSpike ()) {
                System.out.println ("------spike detected------");
                IOLatencyDetector.metricsHis ();
            }
            if (j % 1000 == 0)
                IOLatencyDetector.metricsHis ();
        }
    }
}
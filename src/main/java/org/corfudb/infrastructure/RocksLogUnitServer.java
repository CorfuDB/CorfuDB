/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// @author Amy Tai
//
// implement object homes.
package org.corfudb.infrastructure;

import lombok.Getter;
import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.corfudb.infrastructure.thrift.*;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.logunits.CorfuDBSimpleLogUnitProtocol;
import org.corfudb.runtime.protocols.logunits.IWriteOnceLogUnit;
import org.corfudb.runtime.smr.ICorfuDBObject;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RocksLogUnitServer implements RocksLogUnitService.Iface, ICorfuDBServer {
    private Logger log = LoggerFactory.getLogger(RocksLogUnitServer.class);

    List<Integer> masterIncarnation = null;
    protected int UNITCAPACITY = 100000; // capacity in PAGESIZE units, i.e. UNITCAPACITY*PAGESIZE bytes
    protected int PORT=-1;	// REQUIRED: port number this unit listens on
    protected String DRIVENAME = null; // where to persist data (unless rammode is on)
    protected boolean RAMMODE = true; // command line switch: work in memory (no data persistence)
    protected boolean RECOVERY = false; // command line switch: indicate whether we load stream from disk on startup
    protected boolean REBUILD = false;
    boolean simFailure = false;
    protected String rebuildnode = null;

    protected int PAGESIZE;
    @Getter
    private Thread thread;
    boolean running;
    TServer server;

    private int ckmark = 0; // start offset of latest checkpoint. TODO: persist!!

    private Object DriveLck = new Object();

    private long gcmark = 0; // pages up to 'gcmark' have been evicted; note, we must have gcmark <= CM.trimmark
    private int lowwater = 0, highwater = 0, freewater = -1;

    long highWatermark = -1L;

    private HashMap<Long, Hints> hintMap = new HashMap();
    private RocksDB db = null;
    private AtomicBoolean ready = new AtomicBoolean(); // for testing

    public boolean isReady() {
        return ready.get();
    }

    public void initLogStore(int sz) {
        if (RAMMODE) {
            //TODO: RocksDB in ram-mode?
        }
        UNITCAPACITY = freewater = sz;
        masterIncarnation = new ArrayList<Integer>();
        masterIncarnation.add(0);
    }

    public void initLogStore(byte[] initmap, int sz) throws Exception {
        if (RAMMODE) {
            //TODO: RocksDB in ram-mode?
        }
        UNITCAPACITY = freewater = sz;
        masterIncarnation = new ArrayList<Integer>();
        masterIncarnation.add(0);
    }

    public RocksLogUnitServer() {
        //default constructor
    }

    //TODO: Make this accept an object from an interface, such as IWriteOnceLogUnit?
    private boolean rebuildFrom(CorfuDBSimpleLogUnitProtocol nodeToFetch) {
        SimpleLogUnitWrap data = nodeToFetch.fetchRebuild();

        if (data == null || !data.isSetErr() || !data.getErr().equals(ErrorCode.OK)) {
            log.error("couldn't get rebuild data from node: {}", nodeToFetch.getFullString());
            log.error("data: {}", data);
            return false;
        }

        SimpleLogUnitServer temp = new SimpleLogUnitServer();
        try {
            temp.initLogStore(data.getBmap(), data.getUnitcapacity());
        } catch (Exception ex) {
            log.error("couldn't rebuild log store from bitmap: {}", ex);
            return false;
        }

        long startAddress = data.getLowwater();
        if (!data.isSetCtnt())
            return true;
        for (ByteBuffer bb : data.getCtnt()) {
            //TODO: FIX THE FAKE STREAM!! once simple log unit server gets streams
            try {
                put(startAddress, data.getHintmap().get(startAddress).getNextMap().keySet(), bb, temp.getET(startAddress));
            } catch (IOException e) {
                log.error("Trying to rebuild node, got exception: {}", e);
            }
            startAddress++;
        }
        return true;
    }

    public void simulateFailure(boolean fail, long length)
            throws TException
    {
        if (fail && length != -1)
        {
            this.simFailure = true;
            final RocksLogUnitServer t = this;
            new Timer().schedule(
                    new TimerTask() {
                        @Override
                        public void run() {
                            t.simFailure = false;
                        }
                    },
                    length
            );
        }
        else {
            this.simFailure = fail;
        }
    }

    @Override
    public ICorfuDBServer getInstance (final Map<String,Object> config)
    {
        final RocksLogUnitServer lut = this;

        //These are required and will throw an exception if not defined.
        lut.RAMMODE = (Boolean) config.get("ramdisk");
        lut.UNITCAPACITY = (Integer) config.get("capacity");
        lut.PORT = (Integer) config.get("port");
        lut.PAGESIZE = (Integer) config.get("pagesize");
        lut.gcmark = (Integer) config.get("trim");

        masterIncarnation = new ArrayList<Integer>();
        masterIncarnation.add(0);
        //These are not required and will be only populated if given
        if (config.containsKey("drive"))
        {
            lut.DRIVENAME = (String) config.get("drive");
        }
        if (config.containsKey("recovery"))
        {
            lut.RECOVERY = (Boolean) config.get("recovery");
        }
        if (config.containsKey("rebuild")) {
            lut.REBUILD = true;
            lut.rebuildnode = (String) config.get("rebuild");
        }


        thread = new Thread(this);
        return this;
    }

    @Override
    public void close() {
        running = false;
        server.stop();
    }

    private byte[] getKey(long address, String stream) throws IOException {
        ByteArrayOutputStream bs = new ByteArrayOutputStream();
        ByteBuffer br = ByteBuffer.allocate(Long.BYTES);
        br.putLong(address);

        bs.write(stream.getBytes());
        bs.write(br.array());

        return bs.toByteArray();

    }

    // Assumes each ByteBuffer has length <= PAGESIZE.
    private WriteResult put(long address, Set<String> streams, ByteBuffer buf, ExtntMarkType et) throws IOException {
        // TODO: If streams is null, add to EVERY stream??
        if (streams == null)
            return new WriteResult().setCode(ErrorCode.ERR_BADPARAM);
        for (String stream : streams) {
            byte[] key = getKey(address, stream);

            ByteArrayOutputStream bs = new ByteArrayOutputStream();
            bs.write(buf.array());
            bs.write(et.getValue());
            try {
                byte[] value = db.get(key);
                if (value == null)
                    db.put(key, bs.toByteArray());
                else
                    return new WriteResult().setCode(ErrorCode.ERR_OVERWRITE).setData(ByteBuffer.wrap(value));
            } catch (RocksDBException e) {
                throw new IOException(e.getMessage());
            }
        }
        return new WriteResult().setCode(ErrorCode.OK);
    }

    public void trimLogStore(long toOffset) throws IOException {
        throw new UnsupportedOperationException("trimLogStore not implemented in Rocks-backed server!!");
    }

    public ExtntWrap get(long logOffset, String stream) throws IOException {
        ExtntWrap wr = new ExtntWrap();
        //TODO : figure out trim story
        byte[] key = getKey(logOffset, stream);
        byte[] value = null;
        try {
            value = db.get(key);
        } catch (RocksDBException e) {
            throw new IOException(e.getMessage());
        }

        if (value == null) {
            wr.setInf(new ExtntInfo(logOffset, 0, ExtntMarkType.EX_EMPTY));
            wr.setErr(ErrorCode.ERR_UNWRITTEN);
        } else {
            // Length of the data is -1 because we stick the ET in the last byte.
            // TODO: Check the ET of the value?
            wr.setInf(new ExtntInfo(logOffset, value.length - 1, ExtntMarkType.findByValue(value[value.length - 1])));
            byte[] returnValue = new byte[value.length-1];
            for (int i = 0; i < returnValue.length; i++) {
                returnValue[i] = value[i];
            }
            ArrayList<ByteBuffer> content = new ArrayList<ByteBuffer>();
            content.add(ByteBuffer.wrap(returnValue));
            wr.setCtnt(content);
            wr.setErr(ErrorCode.OK);
        }
        return wr;
    }

    private void writegcmark() throws IOException {
        // TODO what about persisting the configuration??
        throw new UnsupportedOperationException("Haven't implemented writegcmark in Rocks-backed server");
    }

    private void recover() throws Exception {
        throw new UnsupportedOperationException("Haven't implemented recover in Rocks-backed server");
    }
    /*
        private void rebuildfromnode() throws Exception {
            Endpoint cn = Endpoint.genEndpoint(rebuildnode);
            TTransport buildsock = new TSocket(cn.getHostname(), cn.getPort());
            buildsock.open();
            TProtocol prot = new TBinaryProtocol(buildsock);
            TMultiplexedProtocol mprot = new TMultiplexedProtocol(prot, "CONFIG");

            SimpleLogUnitConfigService.Client cl = new SimpleLogUnitConfigService.Client(mprot);
            stream.info("established connection with rebuild-node {}", rebuildnode);
            SimpleLogUnitWrap wr = null;
            try {
                wr = cl.rebuild();
                stream.info("obtained mirror lowwater={} highwater={} trimmark={} ctnt-length={}",
                        wr.getLowwater(), wr.getHighwater(), wr.getTrimmark(), wr.getCtntSize());
                initLogStore(wr.getBmap(), UNITCAPACITY);
                lowwater = highwater = wr.getLowwater();
                gcmark = wr.getTrimmark();
                ckmark = (int)wr.getCkmark();
                put(wr.getCtnt());
                if (highwater != wr.getHighwater())
                    stream.error("rebuildfromnode lowwater={} highwater={} received ({},{})",
                            lowwater, highwater,
                            wr.getLowwater(), wr.getHighwater());
            } catch (TException e) {
                e.printStackTrace();
            }
        }*/
    @Override
    public boolean ping() throws TException {
        if (simFailure)
        {
            throw new TException("Simulated failure mode!");
        }
        return true;
    }

    @Override
    public void setEpoch(long epoch) throws TException {
        if (simFailure)
        {
            throw new TException("Simulated failure mode!");
        }
        Long lEpoch = epoch;
        this.masterIncarnation.set(0, lEpoch.intValue());
    }
    /////////////////////////////////////////////////////////////////////////////////////////////
	/* (non-Javadoc)
	 * implements to CorfuUnitServer.Iface write() method.
	 * @see CorfuUnitServer.Iface#write(ExtntWrap)
	 *
	 * we make great effort for the write to either succeed in full, or not leave any partial garbage behind.
	 * this means that we first check if all the pages to be written are free, and that the incoming entry contains content for each page.
	 * in the event of some error in the middle, we reset any values we already set.
	 */
    @Override
    synchronized public WriteResult write(UnitServerHdr hdr, ByteBuffer ctnt, ExtntMarkType et) throws TException {
        if (simFailure)
        {
            throw new TException("Simulated failure mode!");
        }
        if (Util.compareIncarnations(hdr.getEpoch(), masterIncarnation) < 0) {
            log.info("write request has stale incarnation={} cur incarnation={}",
                    hdr.getEpoch(), masterIncarnation);
            return new WriteResult().setCode(ErrorCode.ERR_STALEEPOCH);
        }

        log.debug("write({} size={} marktype={})", hdr.off, ctnt.capacity(), et);
        try {
            WriteResult wr = put(hdr.off, hdr.streamID, ctnt, et);
            highWatermark = Long.max(highWatermark, hdr.off);
            return wr;
        } catch (IOException e) {
            e.printStackTrace();
            return new WriteResult().setCode(ErrorCode.ERR_IO);
        }
    }

    /**
     * mark an extent 'skipped'
     * @param hdr epoch and offset of the extent
     * @return OK if succeeds in marking the extent for 'skip'
     * 		ERROR_TRIMMED if the extent-range has already been trimmed
     * 		ERROR_OVERWRITE if the extent is occupied (could be a good thing)
     * 		ERROR_FULL if the extent spills over the capacity of the stream
     * @throws TException
     */
    @Override
    synchronized public ErrorCode fix(UnitServerHdr hdr) throws TException {
        if (simFailure)
        {
            throw new TException("Simulated failure mode!");
        }
        return write(hdr, ByteBuffer.allocate(0), ExtntMarkType.EX_SKIP).getCode();
    }

    private ExtntWrap genWrap(ErrorCode err) {
        return new ExtntWrap(err, new ExtntInfo(), new ArrayList<ByteBuffer>());
    }

    private Hints genHint(ErrorCode err) {
        return new Hints(err, new HashMap<String, Long>(), false, null);
    }

    /* (non-Javadoc)
     * @see CorfuUnitServer.Iface#read(org.corfudb.CorfuHeader, ExtntInfo)
     *
     * this method performs actual reading of a range of pages.
     * it fails if any page within range has not been written.
     * it returns OK_SKIP if it finds any page within range which has been junk-filled (i.e., the entire range becomes junked).
     *
     * the method also reads-ahead the subsequent meta-info entry if hdr.readnext is set.
     * if the next meta info record is not available, it returns the current meta-info structure
     *
     *  @param a CorfuHeader describing the range to read
     */
    @Override
    synchronized public ExtntWrap read(UnitServerHdr hdr) throws TException {
        if (simFailure)
        {
            throw new TException("Simulated failure mode!");
        }
        if (Util.compareIncarnations(hdr.getEpoch(), masterIncarnation) < 0) return genWrap(ErrorCode.ERR_STALEEPOCH);
        try {
            return get(hdr.off, hdr.streamID.iterator().next());
        } catch (IOException e) {
            e.printStackTrace();
            return genWrap(ErrorCode.ERR_IO);
        }
    }

    /**
     * wait until any previously written stream entries have been forced to persistent store
     */
    @Override
    synchronized public void sync() throws TException {
        if (simFailure)
        {
            throw new TException("Simulated failure mode!");
        }
        synchronized(DriveLck) { try { DriveLck.wait(); } catch (Exception e) {
            log.error("forcing sync to persistent store failed, quitting");
            System.exit(1);
        }}
    }

    @Override
    synchronized public long querytrim() {
        //return CM.getTrimmark();
        //TODO figure out trim story
        return 0;
    }

    @Override
    synchronized public long highestAddress()
            throws TException {
        if (simFailure)
        {
            throw new TException("Simulated failure mode!");
        }
        return highWatermark;
    }
    @Override
    synchronized public void reset() {
        log.debug("Reset requested, resetting state");
        try {
            if (RAMMODE)
            {
                //TODO: Ram-mode in RocksDB?
                initLogStore(UNITCAPACITY);
                writegcmark();
                highWatermark = -1L;
                hintMap = new HashMap<>();
            }
        }
        catch (Exception e)
        {
            log.error("Error during reset", e);
        }
    }

    @Override
    synchronized public long queryck() {	return ckmark; }

    ErrorCode trim(long toOffset) {
        try {
            trimLogStore(toOffset);
        } catch (IOException e) {
            e.printStackTrace();
            return ErrorCode.ERR_IO;
        }
        if (!RAMMODE) {
            try {
                log.debug("forcing bitmap and gcmark to disk");
                synchronized(DriveLck) {
                    try { DriveLck.wait(); } catch (InterruptedException e) {
                        log.error("forcing sync to persistent store failed, quitting");
                        System.exit(1);
                    }
                }
                writegcmark();
            } catch (IOException e) {
                log.error("writing gcmark failed");
                e.printStackTrace();
                return ErrorCode.ERR_IO;
            }
        }
        return ErrorCode.OK;
    }

    @Override
    synchronized public void ckpoint(UnitServerHdr hdr) throws TException {
        if (simFailure)
        {
            throw new TException("Simulated failure mode!");
        }
        // if (hdr.getEpoch() < epoch) return ErrorCode.ERR_STALEEPOCH;
        log.info("mark latest checkpoint offset={}", hdr.off);
        if (hdr.off > ckmark) ckmark = (int) (hdr.off % UNITCAPACITY);
    }

    //////////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////////////////


    public void serverloop() throws Exception {

        log.warn("@C@ CorfuLoggingUnit starting");

        if (!RAMMODE) {
            RocksDB.loadLibrary();

            Options options = new Options().setCreateIfMissing(true);
            /*options.setAllowMmapReads(true);
            // For easy prefix-lookups.
            options.setMemTableConfig(new HashSkipListMemTableConfig());
            options.setTableFormatConfig(new PlainTableConfig());
            options.useFixedLengthPrefixExtractor(16); // Prefix length in bytes */
            try {
                db = RocksDB.open(options, DRIVENAME);
            } catch (RocksDBException e) {
                e.printStackTrace();
                log.warn("couldn't open rocksdb, exception: {}", e);
                System.exit(1); // not much to do without storage...
            }
        } else {
            //TODO: Rammode in RocksDB?
        }

        if (RECOVERY) {
            recover();
        } else if (REBUILD) {
            CorfuDBSimpleLogUnitProtocol protocol = null;
            try
            {
                // Fix epoch later; but if we set it to -1, this guarantees that any write will trigger a view change,
                // which we want
                //TODO: Fix how CorfuDBSimpleLogUnitProtocol is essentially hardcoded?
                protocol = (CorfuDBSimpleLogUnitProtocol) IServerProtocol.protocolFactory(CorfuDBSimpleLogUnitProtocol.class, rebuildnode, -1);
            }
            catch (Exception ex){
                log.error("Error invoking protocol for protocol: ", ex);
                log.error("Cannot rebuild node");
                System.exit(1);
            }

            if (!rebuildFrom(protocol))
                System.exit(1);
        } else {
            initLogStore(UNITCAPACITY);
            //writegcmark();
        }
        ready.set(true);

        TServerSocket serverTransport;
        System.out.println("run..");

        try {
            serverTransport = new TServerSocket(PORT);

            //LogUnitConfigServiceImpl cnfg = new LogUnitConfigServiceImpl();

            TMultiplexedProcessor mprocessor = new TMultiplexedProcessor();
            mprocessor.registerProcessor("SUNIT", new RocksLogUnitService.Processor<RocksLogUnitServer>(this));
            //TODO: Figure out what the Config service is for a RocksDB indexed server?
            //mprocessor.registerProcessor("CONFIG", new SimpleLogUnitConfigService.Processor<LogUnitConfigServiceImpl>(cnfg));

            server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(mprocessor)
                    .protocolFactory(TCompactProtocol::new)
                    .inputTransportFactory(new TFastFramedTransport.Factory())
                    .outputTransportFactory(new TFastFramedTransport.Factory())
            );
            System.out.println("Starting Corfu storage unit server on multiplexed port " + PORT);

            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        running = true;
        while (running) {
            try {
                this.serverloop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

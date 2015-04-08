package org.corfudb.tests.benchtests;

import org.corfudb.client.ITimestamp;
import org.corfudb.runtime.AbstractRuntime;
import org.corfudb.runtime.CorfuDBObject;
import org.corfudb.runtime.CorfuDBObjectCommand;
import org.corfudb.runtime.DirectoryService;

/**
 * Created by crossbach on 4/7/15.
 */
public class CommandThroughputMicroBenchmark extends MicroBenchmark {

    boolean m_tx;
    OpaqueObject[] m_objects;
    OpaqueCommand[] m_commands;
    long[] m_submit;
    long[] m_endtoend;
    int[] m_cmdidx;
    int[] m_attempts;
    long m_initlatency;

    public static class CommandThroughputMicroBenchmarkFactory implements BenchmarkFactory {
        public MicroBenchmark instantiate(Options options) {
            return new CommandThroughputMicroBenchmark(options);
        }
    }

    public static class OpaqueObject extends CorfuDBObject {
        public OpaqueObject(AbstractRuntime tTR, long oid) {
            super(tTR, oid);
        }
        public void applyToObject(Object o, ITimestamp timestamp) {
            OpaqueCommand oc = (OpaqueCommand) o;
            oc.setReturnValue(null);
            s_commandlatencies[oc.m_id] = curtime() - oc.m_start;
        }
    }

    public static class OpaqueCommand extends CorfuDBObjectCommand {

        boolean m_read;
        public long m_start;
        public int m_id;
        public OpaqueCommand(boolean isRead, int id) {
            m_id = id;
            m_read = isRead;
        }
    }

    public static class SizeableOpaqueObject extends OpaqueObject {
        byte[] m_payload;
        public SizeableOpaqueObject(AbstractRuntime tTR, long oid, int payloadBytes) {
            super(tTR, oid);
            m_payload = new byte[payloadBytes];
        }
        public void applyToObject(Object o, ITimestamp timestamp) {
            if(o instanceof SizeableOpaqueCommand) {
                SizeableOpaqueCommand oc = (SizeableOpaqueCommand) o;
                int max = Math.min(oc.m_payload.length, m_payload.length);
                if (oc.m_read) {
                    for (int i = 0; i < max; i++)
                        m_payload[i] = oc.m_payload[i];
                } else {
                    for (int i = 0; i < max; i++)
                        oc.m_payload[i] = m_payload[i];
                }
                oc.setReturnValue(max);
            } else {
                OpaqueCommand oc = (OpaqueCommand) o;
                oc.setReturnValue(null);
                s_commandlatencies[oc.m_id] = curtime() - oc.m_start;
            }
        }
    }

    public static class SizeableOpaqueCommand extends OpaqueCommand {
        byte[] m_payload;
        public SizeableOpaqueCommand(boolean isRead, int payloadBytes, int id) {
            super(isRead, id);
            m_payload = new byte[payloadBytes];
        }
    }

    public CommandThroughputMicroBenchmark(Options options) { super(options); }

    /**
     * execute the given command
     * @param cmd
     * @return the latency incurred by the command
     */
    protected long
    executeCommand(OpaqueCommand cmd, int objectIdx, int commandIdx) {

        boolean intx = false;
        boolean done = false;
        long start = curtime();
        cmd.m_start = start;

        while(!done) {
            try {
                intx = BeginTX();
                m_attempts[commandIdx]++;
                OpaqueObject o = m_objects[objectIdx];
                if (cmd.m_read) {
                    // inform("thread %d executing a read on oid %d\n", Thread.currentThread().getId(), o.oid);
                    m_rt.query_helper(o, null, cmd);
                } else {
                    // inform("thread %d executing a write on oid %d\n", Thread.currentThread().getId(), o.oid);
                    m_rt.update_helper(o, cmd);
                }
                done = EndTX();
                intx = false;
            } catch (Exception e) {
                done = AbortTX(intx);
                intx = false;
            }
        }

        return curtime() - start;
    }

    /**
     * create a new command.
     * if there is not payload, use the base class to avoid
     * serialization of unused payload member.
     * @param read
     * @param cmdSize
     * @param cmdIdx
     * @return
     */
    protected OpaqueCommand
    createCommand(
            boolean read,
            int cmdSize,
            int cmdIdx
        )
    {
        return cmdSize == 0 ?
                new OpaqueCommand(read, cmdIdx) :
                new SizeableOpaqueCommand(read, cmdSize, cmdIdx);
    }

    /**
     * create a new object.
     * if object size is 0, use base class to
     * circumvent serialization
     * @param nObjectSize
     * @return
     */
    protected OpaqueObject
    createObject(
            int nObjectSize
        ) {
        return nObjectSize == 0 ?
                new OpaqueObject(m_rt, DirectoryService.getUniqueID(m_sf)):
                new SizeableOpaqueObject(m_rt, DirectoryService.getUniqueID(m_sf), nObjectSize);
    }

    /**
     * preconfigure the micro-benchmark so that no allocation
     * occurs on the critical path. set up all the objects
     * and commands in advance so we are looking only at RT
     * overheads when we get to the execute phase.
     */
    public void initialize() {

        long lInitStart = curtime();
        int nObjects = m_options.getObjectCount();
        int nCommands = m_options.getOperationCount();
        int nObjectSize = m_options.getObjectSize();
        int nCommandSize = m_options.getCommandSize();
        double dRWRatio = m_options.getRWRatio();
        m_tx = m_options.getUseTransactions();

        m_objects = new OpaqueObject[nObjects];
        m_commands = new OpaqueCommand[nCommands];
        m_submit = new long[nCommands];
        m_endtoend = new long[nCommands];
        s_commandlatencies = new long[nCommands];
        m_attempts = new int[nCommands];
        m_cmdidx = new int[nCommands];

        for(int i=0; i<nObjects; i++)
            m_objects[i] = createObject(nObjectSize);
        for(int i=0; i<nCommands; i++) {
            m_commands[i] = createCommand(Math.random() < dRWRatio, nCommandSize, i);
            m_cmdidx[i] = (int) Math.floor(Math.random()*nObjects);
            m_submit[i] = -1;
            s_commandlatencies[i] = -1;
            m_attempts[i] = 0;
        }

        m_initlatency = curtime() - lInitStart;
    }

    /**
     * execute a list of pre-created commands,
     * tracking the latency of the command submission,
     * (which may or may not be synchronous with execution)
     */
    public void
    executeImpl(int nStartIdx, int nItems) {

        for(int i=nStartIdx; i<nStartIdx+nItems; i++) {
            int objectIdx = m_cmdidx[i];
            OpaqueObject o = m_objects[objectIdx];
            OpaqueCommand cmd = m_commands[i];
            m_submit[i] = executeCommand(cmd, objectIdx, i);
        }
    }

    public void finalize() {
        int nCommands = m_options.getOperationCount();
        for(int i=0; i<nCommands; i++) {
            m_endtoend[i] = s_commandlatencies[i];
        }
    }

    /**
     * micro-benchmark-specific reporting
     * @param bShowHeaders
     * @return
     */
    public void
    reportImpl(StringBuilder sb, boolean bShowHeaders) {

        if(bShowHeaders) {
            sb.append("Benchmark: ");
            sb.append(m_options.getTestScenario());
            sb.append(" on ");
            sb.append(m_options.getMaster());
            sb.append(" (");
            sb.append(m_options.getRPCHostName());
            sb.append(")\n");
            sb.append("bnc, rt, strmimpl, tx, opaque, rd_my_wr, thrds, objs, cmds, rwpct, objsize, cmdsize, init(msec), exec(msec), tput(tx/s), avg-sublat(usec), avg-cmdlat(usec), retry_per_tx\n");
        }

        int nObjects = m_options.getObjectCount();
        int nCommands = m_options.getOperationCount();
        double dRW = m_options.getRWRatio();
        int nObjSize = m_options.getObjectSize();
        int nCmdSize = m_options.getCommandSize();
        String ustx = m_options.getUseTransactions() ? "tx":"notx";
        long init = msec(m_initlatency);
        long exec = msec(m_exec);
        double tput = (double) nCommands / (((double) exec)/1000.0);

        int nErrorSSamples = 0;
        int nValidSSamples = 0;
        int nErrorE2ESamples = 0;
        int nValidE2ESamples = 0;
        int nReads = 0;
        int nWrites = 0;
        int nValidSReads = 0;
        int nValidSWrites = 0;
        int nValidEReads = 0;
        int nValidEWrites  = 0;

        double avgsub = 0.0;
        double avgrsub = 0.0;
        double avgwsub = 0.0;
        double avge2e = 0.0;
        double avgre2e = 0.0;
        double avgwe2e = 0.0;
        double retpertx = 0.0;

        for(int i=0; i<nCommands; i++) {

            long submitlatency = m_submit[i];
            long e2elatency = m_endtoend[i];
            boolean svalid = submitlatency > 0;
            boolean evalid = e2elatency > 0;
            boolean isread = m_commands[i].m_read;

            nReads += isread ? 1 : 0;
            nWrites += isread ? 0 : 1;
            nErrorSSamples += svalid ? 0 : 1;
            nValidSSamples += svalid ? 1 : 0;
            nErrorE2ESamples += evalid ? 0 : 1;
            nValidE2ESamples += evalid ? 1 : 0;
            nValidEReads += evalid && isread ? 1 : 0;
            nValidEWrites += evalid && !isread ? 1 : 0;
            nValidSReads += svalid && isread ? 1 : 0;
            nValidSWrites += svalid && !isread ? 1 : 0;
            avgsub += svalid ? m_submit[i] : 0;
            avge2e += evalid ? m_endtoend[i] : 0;
            avgrsub += svalid && isread ? m_submit[i] : 0;
            avgwsub += svalid && !isread ? m_submit[i] : 0;
            avgre2e += evalid && isread ? m_endtoend[i] : 0;
            avgwe2e += evalid && !isread ? m_endtoend[i] : 0;
            retpertx += m_attempts[i] - 1;
        }

        avgsub = nValidSSamples == 0 ? 0.0 : avgsub/nValidSSamples;
        avge2e = nValidE2ESamples == 0 ? 0.0 : avge2e/nValidE2ESamples;
        avgrsub = nValidSReads == 0 ? 0.0 : avgrsub/nValidSReads;
        avgwsub = nValidSWrites == 0 ? 0.0 : avgwsub/nValidSWrites;
        avgre2e = nValidEReads == 0 ? 0.0 : avgre2e/nValidEReads;
        avgwe2e = nValidEWrites == 0 ? 0.0 : avgwe2e/nValidEWrites;
        avgsub /= 1000000.0;
        avge2e /= 1000000.0;
        avgrsub /= 1000000.0;
        avgwsub /= 1000000.0;
        avgre2e /= 1000000.0;
        avgwe2e /= 1000000.0;
        retpertx /= nCommands;

        sb.append(String.format("%s, %s, %s, %s, %s, %s, %d, %d, %d, %.2f, %d, %d, %d, %d, %.3f, %.3f, %.3f, %.3f\n",
                m_options.getTestScenario(),
                m_options.getRuntime(),
                m_options.getStreamImplementation(),
                (m_options.getUseTransactions() ? "tx":"notx"),
                (m_options.getOpacity() ? "opaque" : "not-opq"),
                (m_options.getReadMyWrites()? "RMW":"NRMW"),
                m_options.getThreadCount(),
                nObjects,
                nCommands,
                dRW,
                nObjSize,
                nCmdSize,
                init,
                exec,
                tput,
                avgsub,
                avge2e,
                retpertx
        ));

        if(m_options.getCollectLatencyStats()) {
            sb.append(String.format("upd-qry-lat(usec, avg=%.3f), ", avgsub));
            for(int i=0; i<nCommands; i++) {
                sb.append(usec(m_submit[i]));
                sb.append(", ");
            }
            sb.append(String.format("\nquery_helper(avg=%.3f), ", avgrsub));
            for(int i=0; i<nCommands; i++) {
                if(m_commands[i].m_read) {
                    sb.append(usec(m_submit[i]));
                    sb.append(", ");
                }
            }
            sb.append(String.format("\nupdate_helper (avg=%.3f),", avgwsub));
            for(int i=0; i<nCommands; i++) {
                if(!m_commands[i].m_read) {
                    sb.append(usec(m_submit[i]));
                    sb.append(", ");
                }
            }
            sb.append(String.format("\napplyToObject(avg=%.3f), ", avge2e));
            for(int i=0; i<nCommands; i++) {
                sb.append(usec(m_endtoend[i]));
                sb.append(", ");
            }
            sb.append(String.format("\napplyRead(R, avg=%.3f), ", avgre2e));
            for(int i=0; i<nCommands; i++) {
                if(m_commands[i].m_read) {
                    sb.append(usec(m_endtoend[i]));
                    sb.append(", ");
                }
            }
            sb.append(String.format("\napplyWrite(avg=%.3f),", avgwe2e));
            for(int i=0; i<nCommands; i++) {
                if(!m_commands[i].m_read) {
                    sb.append(usec(m_endtoend[i]));
                    sb.append(", ");
                }
            }
        }
    }
}

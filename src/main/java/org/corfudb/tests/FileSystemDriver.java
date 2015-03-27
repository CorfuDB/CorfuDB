package org.corfudb.tests;

import org.corfudb.runtime.Pair;

import java.io.*;
import java.util.*;

public class FileSystemDriver {

    public enum fsoptype {
        access,
        create,
        open,
        close,
        read,
        write,
        resize,
        delete,
        mkdir,
        readdir,
        rmdir,
        search,
        maxoptypes;
        private static fsoptype[] s_vals = values();
        public static fsoptype fromInt(int i) { return s_vals[i]; }
        protected static HashMap<fsoptype, Double> s_thresholds_d = new HashMap<fsoptype, Double>();
        static {
            s_thresholds_d.put(access, 5.0);                          // 5
            s_thresholds_d.put(create, 5.0+ s_thresholds_d.get(access)); // 10
            s_thresholds_d.put(open, 10.0+ s_thresholds_d.get(create));  // 20
            s_thresholds_d.put(close, 10.0+ s_thresholds_d.get(open));   // 30
            s_thresholds_d.put(read, 20.0+ s_thresholds_d.get(close));   // 50
            s_thresholds_d.put(write, 15.0+ s_thresholds_d.get(read));   // 64
            s_thresholds_d.put(resize, 4.0+ s_thresholds_d.get(write));  // 69
            s_thresholds_d.put(delete, 2.0+ s_thresholds_d.get(resize)); // 71
            s_thresholds_d.put(mkdir, 8.0+ s_thresholds_d.get(delete));  // 79
            s_thresholds_d.put(readdir, 8.0+ s_thresholds_d.get(mkdir)); // 87
            s_thresholds_d.put(rmdir, 2.0+ s_thresholds_d.get(readdir)); // 89
            s_thresholds_d.put(search, 100.0);
        }
        protected static HashMap<fsoptype, Double> s_thresholds = new HashMap<fsoptype, Double>();
        static {
            s_thresholds.put(access, 5.0);                          // 5
            s_thresholds.put(create, 3.0+ s_thresholds.get(access)); // 8
            s_thresholds.put(open, 10.0+ s_thresholds.get(create));  // 18
            s_thresholds.put(close, 10.0+ s_thresholds.get(open));   // 28
            s_thresholds.put(read, 30.0+ s_thresholds.get(close));   // 58
            s_thresholds.put(write, 15.0+ s_thresholds.get(read));   // 71
            s_thresholds.put(resize, 4.0+ s_thresholds.get(write));  // 75
            s_thresholds.put(delete, 0.0+ s_thresholds.get(resize)); // 75
            s_thresholds.put(mkdir, 3.0+ s_thresholds.get(delete));  // 78
            s_thresholds.put(readdir, 8.0+ s_thresholds.get(mkdir)); // 86
            s_thresholds.put(rmdir, 0.0+ s_thresholds.get(readdir)); // 86
            s_thresholds.put(search, 100.0);
        }
        protected static Double thresh(fsoptype op) { return s_thresholds.get(op); }
        public static fsoptype randomOp(Random random) {
            Double d = random.nextDouble() * 100.0;
            if(d < thresh(access)) return access;
            if(d < thresh(create)) return create;
            if(d < thresh(open)) return open;
            if(d < thresh(close)) return close;
            if(d < thresh(read)) return read;
            if(d < thresh(write)) return write;
            if(d < thresh(resize)) return resize;
            if(d < thresh(delete)) return delete;
            if(d < thresh(mkdir)) return mkdir;
            if(d < thresh(readdir)) return readdir;
            if(d < thresh(rmdir)) return rmdir;
            return search;
        }
    }

    public static class Op implements Serializable {

        protected transient static HashMap<String, BTreeFS.FSEntry> s_openfiles = new HashMap();
        protected transient static HashMap<String, Integer> s_openstate = new HashMap();
        protected transient static HashSet<String> s_deleted = new HashSet<String>();

        fsoptype optype;
        Map<String, Object> parameters;
        Object result;
        protected transient long lStart;
        protected transient long lComplete;
        public void begin() { lStart = System.currentTimeMillis(); }
        public void complete() { lComplete = System.currentTimeMillis(); }
        public long getRequestLatencyMS() { return lComplete - lStart; }

        public Op(fsoptype _type, Pair<String, Object>... params) {
            lStart = 0;
            lComplete = 0;
            result = null;
            optype = _type;
            parameters = new HashMap<String, Object>();
            for(Pair<String, Object> p : params)
                parameters.put(p.first, p.second);
        }

        /**
         * get a request latency string
         * @return
         */
        public String getRequestStats() {
            StringBuilder sb = new StringBuilder();
            sb.append("FSREQLAT, ");
            sb.append(getRequestLatencyMS());
            sb.append(", \t\"");
            sb.append(toString());
            sb.append("\"");
            return sb.toString();
        }

        /**
         * toString
         * @return
         */
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            switch(optype) {

                case access: {
                    // public int access(String path, int mode);
                    sb.append("access(");
                    sb.append((String) parameters.get("path"));
                    sb.append(", ");
                    sb.append((Integer) parameters.get("mode"));
                    sb.append(")");
                    return sb.toString();
                }

                case create: {
                    // public boolean
                    // create(
                    //        String fsName,
                    //        String parentPath,
                    //        int permissions);
                    sb.append("create(");
                    sb.append((String) parameters.get("fsname"));
                    sb.append(", ");
                    sb.append((String) parameters.get("parentPath"));
                    sb.append(", ");
                    sb.append((Integer) parameters.get("permissions"));
                    sb.append(")");
                    return sb.toString();
                }

                case open: {
                    // public FSEntry open(String path, long mode)
                    sb.append("open(");
                    sb.append((String) parameters.get("path"));
                    sb.append(",");
                    sb.append((Integer) parameters.get("mode"));
                    sb.append(")");
                    return sb.toString();
                }

                case close: {
                    // public void close(FSEntry entry)
                    sb.append("close(");
                    sb.append((String) parameters.get("entry"));
                    sb.append(")");
                    return sb.toString();
                }

                case read: {
                    // public int read(FSEntry file, byte[] buf, int count)
                    sb.append("read(");
                    sb.append((String) parameters.get("file"));
                    sb.append(", ");
                    sb.append((byte[]) parameters.get("buf"));
                    sb.append(",");
                    sb.append((Integer) parameters.get("count"));
                    sb.append(")");
                    return sb.toString();
                }

                case write: {
                    // public int write(FSEntry file, byte[] buf, int count)
                    sb.append("write(");
                    sb.append((String) parameters.get("file"));
                    sb.append(", ");
                    sb.append((Integer) parameters.get("fOffset"));
                    sb.append(", ");
                    sb.append((byte[]) parameters.get("buf"));
                    sb.append(",");
                    sb.append((Integer) parameters.get("bufOffset"));
                    sb.append(", ");
                    sb.append((Integer) parameters.get("count"));
                    sb.append(")");
                    return sb.toString();
                }

                case resize: {
                    // public int resize(FSEntry file, long newlen)
                    sb.append("resize(");
                    sb.append((String) parameters.get("file"));
                    sb.append(", ");
                    sb.append((Long) parameters.get("newlen"));
                    sb.append(")");
                    return sb.toString();
                }

                case delete: {
                    // public boolean delete(String path);
                    sb.append("delete(");
                    sb.append(parameters.get("path"));
                    sb.append(")");
                    return sb.toString();
                }

                case mkdir: {
                    //public boolean
                    //mkdir(String fsName,
                    //      String parentPath,
                    //      int permissions);
                    sb.append("mkdir(");
                    sb.append((String) parameters.get("fsName"));
                    sb.append(", ");
                    sb.append((String) parameters.get("parentPath"));
                    sb.append(", ");
                    sb.append((Integer) parameters.get("permissions"));
                    sb.append(")");
                    return sb.toString();
                }

                case readdir: {
                    // public FSEntry[] readdir(String strPath);
                    sb.append("readdir(");
                    sb.append((String) parameters.get("strPath"));
                    sb.append(")");
                    return sb.toString();
                }

                case rmdir: {
                    // public boolean rmdir(String path);
                    sb.append("rmdir(");
                    sb.append((String) parameters.get("path"));
                    sb.append(")");
                    return sb.toString();
                }

                case search: {
                    // public List<FSEntry> search(String parent, String name);
                    sb.append("search(");
                    sb.append((String) parameters.get("parent"));
                    sb.append(", ");
                    sb.append((String) parameters.get("name"));
                    sb.append(")");
                    return sb.toString();
                }

                default:
                    throw new RuntimeException("unknown op type!");
            }
        }

        public Object
        play(BTreeFS fs) {

            begin();
            switch(optype) {

                case access: {
                    // public int access(String path, int mode)
                    String path = (String) parameters.get("path");
                    Integer mode = (Integer) parameters.get("mode");
                    result = fs.access(path, mode);
                    break;
                }

                case create: {
                    // public boolean
                    // create(
                    //        String fsName,
                    //        String parentPath,
                    //        int permissions);
                    String fsname = (String) parameters.get("fsname");
                    String parentPath = (String) parameters.get("parentPath");
                    Integer permissions = (Integer) parameters.get("permissions");
                    result = new Boolean(fs.create(fsname, parentPath, permissions));
                    break;
                }

                case open: {
                    // public FSEntry open(String path, long mode)
                    String path = (String) parameters.get("path");
                    Integer mode = (Integer) parameters.get("mode");
                    BTreeFS.FSEntry entry = fs.open(path, mode);
                    if(entry != null) {
                        s_openfiles.put(path, entry);
                        incOpenRefCount(path);
                    }
                    result = entry;
                    break;
                }

                case close: {
                    // public void close(FSEntry entry)
                    String strPath = (String) parameters.get("entry");
                    BTreeFS.FSEntry file = s_openfiles.get(strPath);
                    fs.close(file);
                    Integer refcount = decOpenRefCount(file.path());
                    if(refcount <= 0) {
                        s_openfiles.remove(file.path());
                        s_openstate.remove(file.path());
                    }
                    result = null;
                    break;
                }

                case read: {
                    // public int read(FSEntry file, byte[] buf, int count)
                    String strPath = (String) parameters.get("file");
                    BTreeFS.FSEntry file = s_openfiles.get(strPath);
                    byte[] buf = (byte[]) parameters.get("buf");
                    Integer count = (Integer) parameters.get("count");
                    result = new Integer(fs.read(file, buf, count));
                }

                case write: {
                    //     public int  write(FSEntry file, int fOffset, byte[] buf, int bufOffset, int count)
                    String strPath = (String) parameters.get("file");
                    BTreeFS.FSEntry file = s_openfiles.get(strPath);
                    Integer fOffset = (Integer) parameters.get("fOffset");
                    byte[] buf = (byte[]) parameters.get("buf");
                    Integer bufOffset = (Integer) parameters.get("bufOffset");
                    Integer count = (Integer) parameters.get("count");
                    result = new Integer(fs.write(file, fOffset, buf, bufOffset, count));
                    break;
                }

                case resize: {
                    // public int resize(FSEntry file, int newlen)
                    String strPath = (String) parameters.get("file");
                    BTreeFS.FSEntry file = s_openfiles.get(strPath);
                    Long newlen = (Long) parameters.get("newlen");
                    result = new Long(fs.resize(file, newlen));
                    break;
                }

                case delete: {
                    // public boolean delete(String path);
                    String path = (String) parameters.get("path");
                    result = new Boolean(fs.delete(path));
                    break;
                }

                case mkdir: {
                    //public boolean
                    //mkdir(String fsName,
                    //      String parentPath,
                    //      long permissions);
                    String fsName = (String) parameters.get("fsName");
                    String parentPath = (String) parameters.get("parentPath");
                    Integer permissions = (Integer) parameters.get("permissions");
                    result = new Boolean(fs.mkdir(fsName, parentPath, permissions));
                    break;
                }

                case readdir: {
                    // public FSEntry[] readdir(String strPath);
                    String strPath = (String) parameters.get("strPath");
                    result = fs.readdir(strPath);
                    break;
                }

                case rmdir: {
                    // public boolean rmdir(String path);
                    String path = (String) parameters.get("path");
                    result = new Boolean(fs.rmdir(path));
                    break;
                }

                case search: {
                    // public List<FSEntry> search(String parent, String name);
                    String parent = (String) parameters.get("parent");
                    String name = (String) parameters.get("name");
                    result = fs.search(parent, name);
                    break;
                }

                default:
                    throw new RuntimeException("unknown op type!");
            }
            complete();
            return result;
        }

        protected Integer incOpenRefCount(String path) {
            Integer refcount = s_openstate.containsKey(path) ?
                    s_openstate.get(path) : new Integer(0);
            s_openstate.put(path, new Integer(++refcount));
            return refcount;
        }

        protected Integer decOpenRefCount(String path) {
            Integer refcount = s_openstate.get(path);
            s_openstate.put(path, new Integer(--refcount));
            return refcount;
        }

        /**
         * randomly choose a parent path
         * @param random
         * @param fs
         * @return
         */
        protected static String
        randomPath(
                Random random,
                BTreeFS fs,
                BTreeFS.etype type
            ) {
            if (fs == null) {
                // just generate a random path.
                StringBuilder path = new StringBuilder("root");
                while (true) {
                    path.append("/");
                    path.append(randomChildName(random, null));
                    if(random.nextDouble() < 0.5)
                        break;
                }
                return path.toString();
            } else {
                int deletedRetryCount = 0;
                BTreeFS.FSEntry entry = null;
                do {
                    // try to select fs nodes that have not
                    // been deleted or rmdir targets. If we can't find
                    // one after a reasonable number of retries, then
                    // fine, return a path that we know has will have been
                    // removed by the time we attempt the operation currently
                    // under construction.
                    entry = fs.randomSelect(random, type);
                    if(entry != null &&
                        s_deleted.contains(entry.path()) &&
                        deletedRetryCount < 5) {
                        deletedRetryCount++;
                        entry = null;
                    }
                } while (entry == null);
                return entry.path();
            }
        }

        /**
         * return a random name for an FSEntry
         * @return random string (all lower case)
         */
        public static String
        randomChildName(
                Random rnd,
                BTreeFS fs
            ) {
            return randomChildName(rnd, fs, 1.0);
        }

        /**
         * return a random name for an FSEntry
         * @return random string (all lower case)
         */
        public static String
        randomChildName(
                Random rnd,
                BTreeFS fs,
                double dExistsProbability
            ) {
            if(fs == null || rnd.nextDouble() > dExistsProbability) {
                // generate a new identifier randomly
                int minlength = 1;
                int maxlength = 12;
                StringBuilder sb = new StringBuilder();
                int len = (int) ((rnd.nextDouble() * (maxlength - minlength))) + minlength;
                for (int i = 0; i < len; i++) {
                    int cindex = (int) Math.floor(Math.random() * 26);
                    char character = (char) ('a' + cindex);
                    sb.append(character);
                }
                return (sb.toString());
            } else {
                // find a child in the fs tree and return its name.
                BTreeFS.FSEntry entry = null;
                do {
                    entry = fs.randomSelect(rnd, BTreeFS.etype.file);
                } while (entry == null);
                return entry.name;
            }
        }

        /**
         * randomly choose a file
         * @param random
         * @param fs
         * @return
         */
        protected static String
        randomFilePath(Random random, BTreeFS fs) {
            return randomPath(random, fs, BTreeFS.etype.file);
        }

        /**
         * randomly choose a directory path
         * @param random
         * @param fs
         * @return
         */
        protected static String
        randomDirectoryPath(Random random, BTreeFS fs) {
            return randomPath(random, fs, BTreeFS.etype.dir);
        }

        /**
         * random permissions
         * currently, we have 0x1, 0x2, 0x4 for read, write append,
         * so a random number less than 8 should be great.
         * @param random
         * @return
         */
        protected static int randomPermissions(Random random) {
            return random.nextInt(BTreeFS._ACCESS_GLOBAL_EXEC);
        }


        /**
         * return a random file we expect to be open
         * according to previous operations in the trace
         * @param random
         * @return
         */
        protected static String randomOpenFile(Random random) {
            while(s_openstate.size() > 0) {
                int files = s_openstate.size();
                String[] strKeys = new String[files];
                s_openstate.keySet().toArray(strKeys);
                int idx = (int) Math.floor(random.nextDouble() * files);
                String strFilePath = strKeys[idx];
                Integer openCount = s_openstate.get(strFilePath);
                if (openCount > 0)
                    return strFilePath;
                s_openstate.remove(strFilePath);
            }
            return null;
        }

        /**
         * return a random file we expect to be open
         * according to previous operations in the trace
         * @param random
         * @return
         */
        protected static String
        randomNonOpenFile(
                Random random,
                BTreeFS fs
            ) {
            int nRetryCount = 0;
            while(nRetryCount < 10) {
                String strFilePath = randomFilePath(random, fs);
                Integer openCount = s_openstate.get(strFilePath);
                if (openCount == null || openCount == 0)
                    if(!s_deleted.contains(strFilePath))
                        return strFilePath;
                nRetryCount++;
            }
            return null;
        }

        /**
         * return a random buffer
         * @param random
         * @return
         */
        protected static byte[]
        randomBuffer(Random random) {
            byte[] result = new byte[random.nextInt(4096)];
            random.nextBytes(result);
            return result;
        }

        /**
         * synthesize a new operation
         * @param random
         * @param fs
         * @return
         */
        public static Op
        synthesizeOp(Random random, BTreeFS fs) {

            fsoptype optype = fsoptype.randomOp(random);
            switch(optype) {

                case access:
                    // public int access(String path, int mode);
                    String apath = randomFilePath(random, fs);
                    if(apath == null)
                        return null;
                    Integer amode = (Integer) randomPermissions(random);
                    return new Op(optype,
                            new Pair("path", apath),
                            new Pair("mode", amode));

                case create:
                    // public boolean
                    // create(
                    //        String fsName,
                    //        String parentPath,
                    //        int permissions);
                    String parentPath = randomDirectoryPath(random, fs);
                    if(parentPath == null)
                        return null;
                    String fsname = randomChildName(random, null);
                    Integer permissions = (Integer) randomPermissions(random);
                    return new Op(optype,
                                  new Pair("fsname", fsname),
                                  new Pair("parentPath", parentPath),
                                  new Pair("permissions", permissions));

                case open:
                    // public FSEntry open(String path, long mode)
                    String path = randomFilePath(random, fs);
                    if(path == null)
                        return null;
                    Integer mode = (Integer) randomPermissions(random);
                    return new Op(optype,
                                  new Pair("path", path),
                                  new Pair("mode", mode));

                case close:
                    // public void close(FSEntry entry)
                    String file = randomOpenFile(random);
                    if(file == null) return null;
                    return new Op(optype, new Pair("entry", file));

                case read:
                    // public int read(FSEntry file, byte[] buf, int count)
                    String fp = randomOpenFile(random);
                    if(fp == null) return null;
                    byte[] buf = randomBuffer(random);
                    Integer count = buf.length;
                    return new Op(optype,
                                  new Pair("file", fp),
                                  new Pair("buf", buf),
                                  new Pair("count", count));

                case write:
                    // public int write(FSEntry file, byte[] buf, int count)
                    String wfp = randomOpenFile(random);
                    if(wfp == null) return null;
                    byte[] wbuf = randomBuffer(random);
                    Integer wcount = wbuf.length;
                    Integer fOffset = random.nextInt(wcount);
                    Integer bufOffset = random.nextInt(wcount);
                    return new Op(optype,
                            new Pair("file", wfp),
                            new Pair("fOffset", fOffset),
                            new Pair("buf", wbuf),
                            new Pair("bufOffset", bufOffset),
                            new Pair("count", wcount));

                case resize:
                    // public long resize(FSEntry file, long newlen)
                    String tfp = randomOpenFile(random);
                    if(tfp == null) return null;
                    Long newlen = new Long((long)random.nextInt((int)BTreeFS.MAX_EXTENT*4));
                    return new Op(optype,
                                  new Pair("file", tfp),
                                  new Pair("newlen", newlen));

                case delete:
                    // public boolean delete(String path);
                    String dpath = (String) randomNonOpenFile(random, fs);
                    if(dpath == null)
                        return null;
                    s_deleted.add(dpath);
                    return new Op(optype, new Pair("path", dpath));

                case mkdir:
                    //public boolean
                    //mkdir(String fsName,
                    //      String parentPath,
                    //      int permissions);
                    String mparentPath = (String) randomDirectoryPath(random, fs);
                    if(mparentPath == null)
                        return null;
                    String fsName = (String) randomChildName(random, null);
                    Integer mpermissions = (Integer) randomPermissions(random);
                    return new Op(optype,
                                  new Pair("fsName", fsName),
                                  new Pair("parentPath", mparentPath),
                                  new Pair("permissions", mpermissions));


                case readdir:
                    // public FSEntry[] readdir(String strPath);
                    String rpath = (String) randomDirectoryPath(random, fs);
                    if(rpath == null)
                        return null;
                    return new Op(optype, new Pair("strPath", rpath));

                case rmdir:
                    // public boolean rmdir(String path);
                    String rmpath = (String) randomDirectoryPath(random, fs);
                    if(rmpath == null) return null;
                    return new Op(optype, new Pair("path", rmpath));

                case search: {
                    // public List<FSEntry> search(String parent, String name);
                    String parent = (String) randomDirectoryPath(random, fs);
                    if(parent == null) return null;
                    String name = (String) randomChildName(random, fs, 0.5);
                    return new Op(optype,
                                  new Pair("parent", parent),
                                  new Pair("name", name));
                }

                default:
                    throw new RuntimeException("unknown op type!");
            }
        }
    }

    public static class FileSystemWorkload implements Serializable {

        protected Op[] m_ops;
        protected int m_cur;
        protected int m_nOps;

        /**
         * ctor
         * @param fs
         * @param nOps
         */
        public
        FileSystemWorkload(
                BTreeFS fs,
                int nOps
            ) {
            m_ops = null;
            m_cur = 0;
            m_nOps = nOps;
        }

        /**
         * ctor
         * @param fs
         * @param ops
         */
        public
        FileSystemWorkload(
                BTreeFS fs,
                Collection<Op> ops
            ) {
            m_ops = new Op[ops.size()];
            m_cur = 0;
            m_nOps = ops.size();
            int i = 0;
            for(Op op : ops) {
                m_ops[i++] = op;
            }
        }

        /**
         * initialize the workload
         * @param seed
         */
        public boolean
        Synthesize(BTreeFS fs, long seed) {
            Random random = new Random(seed);
            m_ops = new Op[m_nOps];
            final int maxRetries = 25;
            for(int i=0; i<m_nOps; i++) {
                Op op = null;
                int retries = 0;
                do {
                    op = Op.synthesizeOp(random, fs);
                    if(op != null) {
                        m_ops[i] = op;
                    }
                    if(++retries >= maxRetries)
                        return false;
                } while(op == null);
            }
            return true;
        }

        protected void reset() { m_cur = 0; }
        protected void set(int pos) { m_cur = pos; }
        protected boolean hasNext() { return m_cur < m_ops.length;}
        protected Op next() { return m_ops[m_cur++]; }

        public String toString() {
            StringBuilder sb = new StringBuilder("WORKLOAD:\n");
            for(int i=0; i<m_nOps; i++) {
                sb.append(i);
                sb.append("\t");
                sb.append(m_ops[i]);
                sb.append("\n");
            }
            return sb.toString();
        }
    }

    protected BTreeFS m_fs;
    protected FileSystemWorkload m_wkld;
    protected FileSystemWorkload m_init;
    protected FileSystemWorkload[] m_phases;
    protected int m_curphase;
    protected int m_curop;
    protected int m_nTotalOps;
    protected int m_nEpochOps;
    protected int m_nEpochNum;
    protected long m_lEpochStart;
    protected final int EPOCH_SIZE = 10;

    /**
     * randomized ctor
     * @param fs
     * @param nOps
     */
    public
    FileSystemDriver(BTreeFS fs, int nOps) {
        m_fs = fs;
        m_init = null;
        m_wkld = new FileSystemWorkload(m_fs, nOps);
        m_phases = new FileSystemWorkload[2];
        m_phases[0] = m_init;
        m_phases[1] = m_wkld;
        m_curphase = 1;
        m_curop = 0;
        m_nTotalOps = 0;
        m_nEpochOps = 0;
        m_lEpochStart = 0;
        m_nEpochNum = 0;
    }

    /**
     * ctor from in-memory op lists
     * @param fs
     * @param init
     * @param wkld
     */
    public
    FileSystemDriver(
            BTreeFS fs,
            FileSystemWorkload init,
            FileSystemWorkload wkld
        ) {
        m_fs = fs;
        m_init = init;
        m_wkld = wkld;
        m_phases[0] = m_init;
        m_phases[1] = m_wkld;
        m_curphase = m_init == null ? 0 : 1;
        m_curop = 0;
        m_nTotalOps = 0;
        m_nEpochOps = 0;
        m_lEpochStart = 0;
        m_nEpochNum = 0;
    }

    /**
     * ctor from file system
     * @param fs
     * @param initPath
     * @param wkldPath
     */
    public FileSystemDriver(
            BTreeFS fs,
            String initPath,
            String wkldPath
        ) {
        m_fs = fs;
        Load(initPath, wkldPath);
        m_phases[0] = m_init;
        m_phases[1] = m_wkld;
        m_curphase = m_init == null ? 0 : 1;
        m_curop = 0;
        m_nTotalOps = 0;
        m_nEpochOps = 0;
        m_lEpochStart = 0;
        m_nEpochNum = 0;
    }

    /**
     * synthesize a workload
     * @param nRandomSeed
     * @return
     */
    public boolean SynthesizeWorkload(int nRandomSeed) {
        return m_wkld.Synthesize(m_fs, nRandomSeed);
    }

    /**
     * load the init phase and workload phases
     * @param initPath
     * @param wkldPath
     */
    public void
    Load(String initPath, String wkldPath) {

        try(InputStream initFile = new FileInputStream(initPath);
            InputStream wkldFile = new FileInputStream(wkldPath);
            InputStream initBuffer = new BufferedInputStream(initFile);
            InputStream wkldBuffer = new BufferedInputStream(wkldFile);
            ObjectInput initInput = new ObjectInputStream(initBuffer);
            ObjectInput wkldInput = new ObjectInputStream(wkldBuffer);) {
            m_init = (FileSystemWorkload) initInput.readObject();
            m_wkld = (FileSystemWorkload) wkldInput.readObject();
            m_phases = new FileSystemWorkload[2];
            m_phases[0] = m_init;
            m_phases[1] = m_wkld;
        } catch(ClassNotFoundException ex) {
            System.out.println("failed to load wkld:\n"+ex.getMessage());
            ex.printStackTrace();
        } catch(IOException io) {
            System.out.println("failed to load wkld:\n"+io.getMessage());
            io.printStackTrace();
        }
    }

    public static void
    PersistFSSnapshot(
            String strPhasePath,
            String strPhaseState
        ) {
        try {
            File phaseOut = new File(strPhasePath + ".txt");
            FileOutputStream fosPhase = new FileOutputStream(phaseOut);
            OutputStreamWriter oswPhase = new OutputStreamWriter(fosPhase);
            oswPhase.write(strPhaseState);
            oswPhase.write("\n");
            oswPhase.close();
        } catch (IOException io) {
            System.out.println("failed to save phase FS state:\n"+io.getMessage());
            io.printStackTrace();
        }
    }

    /**
     * persist the init and workload phases
     * @param initPath
     * @param wkldPath
     */
    public void
    Persist(
            String initPath,
            String wkldPath,
            String initFSstate,
            String wkldFSstate
            ) {

        try(OutputStream initFile = new FileOutputStream(initPath);
            OutputStream wkldFile = new FileOutputStream(wkldPath);
            OutputStream initBuffer = new BufferedOutputStream(initFile);
            OutputStream wkldBuffer = new BufferedOutputStream(wkldFile);
            ObjectOutput initOutput = new ObjectOutputStream(initBuffer);
            ObjectOutput wkldOutput = new ObjectOutputStream(wkldBuffer);) {
            initOutput.writeObject(m_init);
            wkldOutput.writeObject(m_wkld);
            PersistFSSnapshot(initPath, initFSstate);
            PersistFSSnapshot(wkldPath, wkldFSstate);
        } catch(IOException io) {
            System.out.println("failed to save wkld:\n"+io.getMessage());
            io.printStackTrace();
        }
    }

    /**
     * pick up where we left off when a client crashed.
     * @param fs
     * @param initPath
     * @param wkldPath
     * @param curPhase
     * @param curOp
     * @return
     */
    public static FileSystemDriver
    Recover(BTreeFS fs,
            String initPath,
            String wkldPath,
            int curPhase,
            int curOp) {
        FileSystemDriver fsw = new FileSystemDriver(fs, initPath, wkldPath);
        fsw.m_curphase = curPhase;
        fsw.m_curop = curOp;
        fsw.m_phases[curPhase].set(fsw.m_curop);
        return fsw;
    }

    protected int pathDepth(String path) {
        if(path == null) return 0;
        String slashlesspath = (new String(path)).replace("/", "");
        return path.length() - slashlesspath.length();
    }

    /**
     * initialization operations arrive ordered in accordance with
     * the order in which the file system tree was generated: in BTreeFS,
     * this typically means *randomly* generated, which in turn corresponds
     * to a depth-first traversal; not necessarily the best order to play
     * them back. Try re-ordering them to enable a breadth first traversal
     * when re-building the tree.
     * @param ops
     */
    protected Collection<Op>
    reorderInitOpsDirsFirst(Collection<Op> ops) {
        Map<Integer, ArrayList<Op>> dirops = new HashMap();
        Map<Integer, ArrayList<Op>> fileops = new HashMap();
        ArrayList<Map<Integer, ArrayList<Op>>> maps = new ArrayList<>();
        maps.add(dirops);
        maps.add(fileops);
        for(Op op : ops) {
            String path = (String) op.parameters.get("parentPath");
            Integer depth = pathDepth(path);
            Map<Integer, ArrayList<Op>> map = (op.optype == fsoptype.mkdir) ? dirops : fileops;
            ArrayList<Op> dlist = map.getOrDefault(depth, new ArrayList());
            map.put(depth, dlist);
            dlist.add(op);
        }
        ArrayList<Op> reordered = new ArrayList<>();
        for(Map<Integer, ArrayList<Op>> map : maps) {
            for (Integer i : map.keySet()) {
                for (Op op : map.get(i)) {
                    reordered.add(op);
                }
            }
        }
        return reordered;
    }

    /**
     * initialization operations arrive ordered in accordance with
     * the order in which the file system tree was generated: in BTreeFS,
     * this typically means *randomly* generated, which in turn corresponds
     * to a depth-first traversal; not necessarily the best order to play
     * them back. Try re-ordering them to enable a breadth first traversal
     * when re-building the tree.
     * @param ops
     */
    protected Collection<Op>
    reorderInitOps(Collection<Op> ops) {
        Map<Integer, ArrayList<Op>> map = new HashMap();
        for (Op op : ops) {
            String path = (String) op.parameters.get("parentPath");
            Integer depth = pathDepth(path);
            ArrayList<Op> dlist = map.getOrDefault(depth, new ArrayList());
            map.put(depth, dlist);
            dlist.add(op);
        }
        ArrayList<Op> reordered = new ArrayList<>();
        for (Integer i : map.keySet()) {
            for (Op op : map.get(i)) {
                reordered.add(op);
            }
        }
        return reordered;
    }

    /**
     * set the init phase up after the fact
     * @param ops
     */
    public void setInitOps(Collection<Op> ops) {
        m_init = new FileSystemWorkload(m_fs, reorderInitOps(ops));
        m_phases[0] = m_init;
    }

    /**
     * toString
     * @return
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if(m_init != null) {
            sb.append("INIT:\n");
            sb.append(m_init.toString());
        }
        sb.append("WKLD:\n");
        sb.append(m_wkld.toString());
        return sb.toString();
    }

    /**
     * initialize the workload by
     * playing its init phase
     */
    public void init() {
        if(m_init != null) {
            boolean oTx = m_fs.m_transactional;
            m_fs.m_transactional = false;
            m_init.reset();
            m_curphase = 0;
            m_curop = 0;
            while (hasNext()) {
                Op op = next();
                System.out.println("initializing: " + op + "...");
                op.play(m_fs);
                m_curop++;
            }
            m_fs.m_transactional = oTx;
        }
    }

    /**
     * "play" the workload
     */
    public void play() {
        init();
        m_wkld.reset();
        m_curphase = 1;
        m_curop = 0;
        m_lEpochStart = System.currentTimeMillis();
        while(hasNext()) {
            Op op = next();
            System.out.println("playing " + op + "...");
            op.play(m_fs);
            m_curop++;
            m_nTotalOps++;
            if(++m_nEpochOps == EPOCH_SIZE || m_curop == m_wkld.m_nOps) {
                long lEpochEnd = System.currentTimeMillis();
                long lEpochMS = lEpochEnd - m_lEpochStart;
                double tput = (double) (m_nEpochOps * 1000.0) / ((double) lEpochMS);
                System.out.format("TPUT, %d, %d %d, %.3f, %d, %d\n",
                                  m_nEpochNum++,
                                  m_nEpochOps,
                                  lEpochMS,
                                  tput,
                                  m_nTotalOps,
                                  lEpochEnd);
                m_nEpochOps = 0;
                m_lEpochStart = lEpochEnd;
            }
        }
    }

    /**
     * "play" the workload forward to given op number
     */
    public void playTo(int nLastOp, boolean init) {
        if(init)
            init();
        m_wkld.reset();
        m_curphase = 1;
        m_curop = 0;
        m_lEpochStart = System.currentTimeMillis();
        while(hasNext() && m_curop < nLastOp) {
            Op op = next();
            System.out.println("playing " + op + "...");
            op.play(m_fs);
            System.out.println(op.getRequestStats());
            m_curop++;
            m_nTotalOps++;
            if(++m_nEpochOps == EPOCH_SIZE || m_curop == nLastOp) {
                long lEpochEnd = System.currentTimeMillis();
                long lEpochMS = lEpochEnd - m_lEpochStart;
                double tput = (double) (m_nEpochOps * 1000.0) / ((double) lEpochMS);
                System.out.format("TPUT, %d, %d %d, %.3f, %d, %d\n",
                        m_nEpochNum++,
                        m_nEpochOps,
                        lEpochMS,
                        tput,
                        m_nTotalOps,
                        lEpochEnd);
                m_nEpochOps = 0;
                m_lEpochStart = lEpochEnd;
            }
        }
    }

    /**
     * "play" the workload forward from the given op number
     */
    public void playFrom(int nFirstOp) {
        m_wkld.set(nFirstOp);
        m_curphase = 1;
        m_curop = nFirstOp;
        m_lEpochStart = System.currentTimeMillis();
        m_nTotalOps = nFirstOp - 1;
        while(hasNext()) {
            Op op = next();
            System.out.println("playing " + op + "...");
            op.play(m_fs);
            System.out.println(op.getRequestStats());
            m_curop++;
            m_nTotalOps++;
            if(++m_nEpochOps == EPOCH_SIZE || m_curop == m_wkld.m_nOps) {
                long lEpochEnd = System.currentTimeMillis();
                long lEpochMS = lEpochEnd - m_lEpochStart;
                double tput = (double) (m_nEpochOps * 1000.0) / ((double) lEpochMS);
                System.out.format("TPUT, %d, %d %d, %.3f, %d, %d\n",
                        m_nEpochNum++,
                        m_nEpochOps,
                        lEpochMS,
                        tput,
                        m_nTotalOps,
                        lEpochEnd);
                m_nEpochOps = 0;
                m_lEpochStart = lEpochEnd;
            }
        }
    }

    /**
     * play a specific phase
     * @param nPhase
     */
    public void playPhase(int nPhase) {
        FileSystemWorkload phase = m_phases[nPhase];
        if(phase == null)
            throw new RuntimeException("phase " + nPhase + " doesn't exist!");
        phase.reset();
        int curop = 0;
        while(phase.hasNext()) {
            Op op = phase.next();
            System.out.format("phase-%d[%d]: %s\n", nPhase, curop, op.toString());
            op.play(m_fs);
            curop++;
        }
    }


    protected boolean hasNext() { return m_phases[m_curphase].hasNext(); }
    protected Op next() { return m_phases[m_curphase].next(); }
    public static Op newMkdirOp(String n, String p, Integer l) { return new Op(fsoptype.mkdir, new Pair("fsName", n), new Pair("parentPath", p), new Pair("permissions", l)); }
    public static Op newCreateOp(String n, String p, Integer l) { return new Op(fsoptype.create, new Pair("fsname", n), new Pair("parentPath", p), new Pair("permissions", l)); }


}

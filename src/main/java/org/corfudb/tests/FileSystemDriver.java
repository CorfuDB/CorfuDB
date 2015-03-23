package org.corfudb.tests;

import org.corfudb.runtime.Pair;

import java.io.Serializable;
import java.util.*;

public class FileSystemDriver {

    public enum fsoptype {
        create,
        open,
        close,
        read,
        write,
        trunc,
        delete,
        mkdir,
        readdir,
        rmdir,
        search,
        maxoptypes;
        private static fsoptype[] s_vals = values();
        public static fsoptype fromInt(int i) { return s_vals[i]; }
    }


    public static class Op implements Serializable {

        fsoptype optype;
        Map<String, Object> parameters;
        Object result;
        protected static HashMap<String, BTreeFS.FSEntry> s_openfiles = new HashMap();
        protected static HashMap<String, Integer> s_openstate = new HashMap();

        public Op(fsoptype _type, Pair<String, Object>... params) {
            result = null;
            optype = _type;
            parameters = new HashMap<String, Object>();
            for(Pair<String, Object> p : params)
                parameters.put(p.first, p.second);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            switch(optype) {

                case create: {
                    // public boolean
                    // create(
                    //        String fsName,
                    //        String parentPath,
                    //        long permissions);
                    sb.append("create(");
                    sb.append((String) parameters.get("fsName"));
                    sb.append(", ");
                    sb.append((String) parameters.get("parentPath"));
                    sb.append(", ");
                    sb.append((Long) parameters.get("permissions"));
                    sb.append(")");
                    return sb.toString();
                }

                case open: {
                    // public FSEntry open(String path, long mode)
                    sb.append("open(");
                    sb.append((String) parameters.get("path"));
                    sb.append(",");
                    sb.append((Long) parameters.get("mode"));
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
                    sb.append((byte[]) parameters.get("buf"));
                    sb.append(",");
                    sb.append((Integer) parameters.get("count"));
                    sb.append(")");
                    return sb.toString();
                }

                case trunc: {
                    // public int trunc(FSEntry file, int newlen)
                    sb.append("trunc(");
                    sb.append((String) parameters.get("file"));
                    sb.append(", ");
                    sb.append((Integer) parameters.get("newlen"));
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
                    //      long permissions);
                    sb.append("mkdir(");
                    sb.append((String) parameters.get("fsName"));
                    sb.append(", ");
                    sb.append((String) parameters.get("parentPath"));
                    sb.append(", ");
                    sb.append((Long) parameters.get("permissions"));
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

            switch(optype) {

                case create: {
                    // public boolean
                    // create(
                    //        String fsName,
                    //        String parentPath,
                    //        long permissions);
                    String fsname = (String) parameters.get("fsname");
                    String parentPath = (String) parameters.get("parentPath");
                    Long permissions = (Long) parameters.get("permissions");
                    result = new Boolean(fs.create(fsname, parentPath, permissions));
                    break;
                }

                case open: {
                    // public FSEntry open(String path, long mode)
                    String path = (String) parameters.get("path");
                    Long mode = (Long) parameters.get("mode");
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
                    // public int write(FSEntry file, byte[] buf, int count)
                    String strPath = (String) parameters.get("file");
                    BTreeFS.FSEntry file = s_openfiles.get(strPath);
                    byte[] buf = (byte[]) parameters.get("buf");
                    Integer count = (Integer) parameters.get("count");
                    result = new Integer(fs.write(file, buf, count));
                    break;
                }

                case trunc: {
                    // public int trunc(FSEntry file, int newlen)
                    String strPath = (String) parameters.get("file");
                    BTreeFS.FSEntry file = s_openfiles.get(strPath);
                    Integer newlen = (Integer) parameters.get("newlen");
                    result = new Integer(fs.trunc(file, newlen));
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
                    Long permissions = (Long) parameters.get("permissions");
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
                BTreeFS.FSEntry entry = null;
                do {
                    entry = fs.randomSelect(random, type);
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
        protected static long randomPermissions(Random random) {
            return random.nextInt(8);
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
            while(true) {
                String strFilePath = randomFilePath(random, fs);
                Integer openCount = s_openstate.get(strFilePath);
                if (openCount == null || openCount == 0)
                    return strFilePath;
            }
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

            fsoptype optype = fsoptype.fromInt(random.nextInt(fsoptype.maxoptypes.ordinal()));
            switch(optype) {

                case create:
                    // public boolean
                    // create(
                    //        String fsName,
                    //        String parentPath,
                    //        long permissions);
                    String parentPath = randomDirectoryPath(random, fs);
                    String fsname = randomChildName(random, null);
                    Long permissions = (Long) randomPermissions(random);
                    return new Op(optype,
                                  new Pair("fsname", fsname),
                                  new Pair("parentPath", parentPath),
                                  new Pair("permissions", permissions));

                case open:
                    // public FSEntry open(String path, long mode)
                    String path = randomFilePath(random, fs);
                    Long mode = (Long) randomPermissions(random);
                    return new Op(optype,
                                  new Pair("path", path),
                                  new Pair("mode", mode));

                case close:
                    // public void close(FSEntry entry)
                    String file = randomOpenFile(random);
                    return new Op(optype, new Pair("entry", file));

                case read:
                    // public int read(FSEntry file, byte[] buf, int count)
                    String fp = randomOpenFile(random);
                    byte[] buf = randomBuffer(random);
                    Integer count = buf.length;
                    return new Op(optype,
                                  new Pair("file", fp),
                                  new Pair("buf", buf),
                                  new Pair("count", count));

                case write:
                    // public int write(FSEntry file, byte[] buf, int count)
                    String wfp = randomOpenFile(random);
                    byte[] wbuf = randomBuffer(random);
                    Integer wcount = wbuf.length;
                    return new Op(optype,
                            new Pair("file", wfp),
                            new Pair("buf", wbuf),
                            new Pair("count", wcount));

                case trunc:
                    // public int trunc(FSEntry file, int newlen)
                    String tfp = randomOpenFile(random);
                    Integer newlen = (Integer) random.nextInt(4096);
                    return new Op(optype,
                                  new Pair("file", tfp),
                                  new Pair("newlen", newlen));

                case delete:
                    // public boolean delete(String path);
                    String dpath = (String) randomNonOpenFile(random, fs);
                    return new Op(optype, new Pair("path", dpath));

                case mkdir:
                    //public boolean
                    //mkdir(String fsName,
                    //      String parentPath,
                    //      long permissions);
                    String fsName = (String) randomChildName(random, null);
                    String mparentPath = (String) randomDirectoryPath(random, fs);
                    Long mpermissions = (Long) randomPermissions(random);
                    return new Op(optype,
                                  new Pair("fsName", fsName),
                                  new Pair("parentPath", mparentPath),
                                  new Pair("permissions", mpermissions));


                case readdir:
                    // public FSEntry[] readdir(String strPath);
                    String rpath = (String) randomDirectoryPath(random, fs);
                    return new Op(optype, new Pair("strPath", rpath));

                case rmdir:
                    // public boolean rmdir(String path);
                    String rmpath = (String) randomDirectoryPath(random, fs);
                    return new Op(optype, new Pair("path", rmpath));

                case search: {
                    // public List<FSEntry> search(String parent, String name);
                    String parent = (String) randomDirectoryPath(random, fs);
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
         * @param seed
         */
        public
        FileSystemWorkload(
                BTreeFS fs,
                int nOps,
                long seed
            ) {
            m_ops = null;
            m_cur = 0;
            m_nOps = nOps;
            initialize(fs, seed);
        }

        /**
         * initialize the workload
         * @param seed
         */
        protected void initialize(BTreeFS fs, long seed) {
            Random random = new Random(seed);
            m_ops = new Op[m_nOps];
            for(int i=0; i<m_nOps; i++) {
                m_ops[i] = Op.synthesizeOp(random, fs);
            }
        }

        protected void reset() { m_cur = 0; }
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

    public
    FileSystemDriver(BTreeFS fs, int nOps, long seed) {
        m_fs = fs;
        m_wkld = new FileSystemWorkload(m_fs, nOps, seed);
    }

    public String toString() {
        return m_wkld.toString();
    }


    /**
     * "play" the workload
     */
    public void play() {
        m_wkld.reset();
        while(hasNext()) {
            Op op = next();
            System.out.println("playing " + op + "...");
            op.play(m_fs);
        }
    }
    protected boolean hasNext() { return m_wkld.hasNext(); }
    protected Op next() { return m_wkld.next(); }

}

package org.corfudb.tests;
import org.corfudb.runtime.*;
import org.corfudb.runtime.collections.CDBAbstractBTree;
import org.corfudb.runtime.collections.CDBLogicalBTree;
import org.corfudb.runtime.collections.CDBPhysicalBTree;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;


public class BTreeFS {

    public enum etype {
        dir,
        file
    }

    public static final int DEFAULT_MINATOM = 1;
    public static final int DEFAULT_MAXATOM = 12;
    public static final int DEFAULT_MAXDIRENTIES = 32;
    public static final double DEFAULT_DIR_PROBABILITY = 0.3;
    public static final int DEFAULT_MAX_HEIGHT = 8;
    public static final long BEGINNING_OF_TIME = -946771200000L + ((60L * 365 * 24 * 60 * 60 * 1000)); // -946771200000L = January 1, 1940
    public static final long MAX_EXTENT = 65536;
    public static final int _ACCESS_USER_READ = (0x1 << 0);
    public static final int _ACCESS_USER_WRITE = (0x1 << 1);
    public static final int _ACCESS_USER_EXEC = (0x1 << 2);
    public static final int _ACCESS_GROUP_READ = (0x1 << 3);
    public static final int _ACCESS_GROUP_WRITE = (0x1 << 4);
    public static final int _ACCESS_GROUP_EXEC = (0x1 << 5);
    public static final int _ACCESS_GLOBAL_READ = (0x1 << 6);
    public static final int _ACCESS_GLOBAL_WRITE = (0x1 << 7);
    public static final int _ACCESS_GLOBAL_EXEC = (0x1 << 8);
    public static final int _ACCESS_DIRECTORY = (0x1 << 9);
    public static final int _ACCESS_DEFAULTS = _ACCESS_USER_READ | _ACCESS_USER_WRITE | _ACCESS_USER_EXEC;

    protected CDBAbstractBTree<String, FSEntry> m_btree;
    protected AbstractRuntime m_rt;
    protected IStreamFactory m_sf;
    protected int m_nMinAtom;
    protected int m_nMaxAtom;
    protected int m_nMaxDirEntries;
    protected int m_nMaxHeight;
    public AtomicLong m_puts;
    public AtomicLong m_gets;
    public AtomicLong m_removes;
    public AtomicLong m_updates;
    protected boolean m_transactional;

    public static class Attrs implements Serializable {

        public long access;
        public long create;
        public long modified;
        public int permissions;

        /**
         * ctor
         * @param p - permissions
         * @param type - dir/file?
         */
        public Attrs(int p, etype type) {
            this(BEGINNING_OF_TIME, BEGINNING_OF_TIME, BEGINNING_OF_TIME, p, type);
        }

        /**
         * ctor
         * @param a - access time/date
         * @param c - create time/date
         * @param m - modify time/date
         * @param p - permissions
         * @param type - dir/file?
         */
        public Attrs(Date a, Date c, Date m, int p, etype type) {
            this(a.getTime(), c.getTime(), m.getTime(), p, type);
        }

        /**
         * ctor
         * @param a - access time/date
         * @param c - create time/date
         * @param m - modify time/date
         * @param p - permissions
         * @param type - dir/file?
         */
        public Attrs(long a, long c, long m, int p, etype type) {
            access = a;
            create = c;
            modified = m;
            permissions = p;
            if(type == etype.dir)
                permissions |= _ACCESS_DIRECTORY;
        }

        /**
         * return unix-style permissions strings
         * @return
         */
        public String getAccessString() {
            StringBuilder sb = new StringBuilder();
            sb.append((permissions &_ACCESS_DIRECTORY) == 0 ? "-":"d");
            sb.append((permissions &_ACCESS_GLOBAL_READ) == 0 ? "-":"r");
            sb.append((permissions &_ACCESS_GLOBAL_WRITE) == 0 ? "-":"w");
            sb.append((permissions &_ACCESS_GLOBAL_EXEC) == 0 ? "-":"x");
            sb.append((permissions &_ACCESS_GROUP_READ) == 0 ? "-":"r");
            sb.append((permissions &_ACCESS_GROUP_WRITE) == 0 ? "-":"w");
            sb.append((permissions &_ACCESS_GROUP_EXEC) == 0 ? "-":"x");
            sb.append((permissions &_ACCESS_USER_READ) == 0 ? "-":"r");
            sb.append((permissions &_ACCESS_USER_WRITE) == 0 ? "-":"w");
            sb.append((permissions &_ACCESS_USER_EXEC) == 0 ? "-":"x");
            return sb.toString();
        }

        /**
         * toString: for now just returns a permissions string
         * TODO: implement date support too.
         * @return
         */
        public String toString() {
            return getAccessString();
        }

    }

    public static class Extent implements Serializable {

        public long block;
        public long offset;
        public long length;

        /**
         * ctor
         * @param rnd
         */
        public Extent(Random rnd) {
            block = rnd.nextLong();
            offset = (long) (rnd.nextDouble() * MAX_EXTENT);
            length = (long) (rnd.nextDouble() * MAX_EXTENT);
        }

        /**
         * ctor
         * @param l
         * @param p
         * @param _len
         */
        public Extent(long l, long p, long _len) {
            block = l;
            offset = p;
            length = _len;
        }

        /**
         * to String
         * @return
         */
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("0x%08x", block));
            sb.append("  ");
            sb.append(offset);
            sb.append("\t");
            sb.append(length);
            return sb.toString();
        }
    }

    public static class FSEntry implements Serializable {

        public static final int VINCREMENT = 8;
        public String parentPath;
        public String name;
        public Attrs attrs;
        public etype type;
        private int nChildren;
        private int nExtents;
        private int nAncestorPermissions;
        private int nAllocChildren;
        private int nAllocExtents;
        private int nAllocAncestorPermissions;
        private String[] vChildren;
        private Extent[] vExtents;
        private Attrs[] vAncestorPermissions;
        public int nOpenForRead;
        public int nOpenForWrite;
        public int nOpenForAppend;
        public int nRefCount;

        public String getChild(int i) { return i < nChildren ? vChildren[i] : null; }
        public void setChild(int i, String s) { vChildren[i] = s; }
        public int numChildren() { return nChildren; }
        public int numAncestors() { return nAncestorPermissions; }
        public Attrs getAncestorPermissions(int i) { return i < nAncestorPermissions ? vAncestorPermissions[i] : null; }
        public int addref() { return ++nRefCount; }
        public int releaseref() { if(--nRefCount < 0) nRefCount = 0; return nRefCount; }
        public int refcount() { return nRefCount; }

        /**
         * append an ancestor permissions entry
         * @param attrs
         */
        protected void addAncestorPermissions(Attrs attrs) {
            if(nAncestorPermissions >= nAllocAncestorPermissions) {
                int nNewAllocAncestors = nAllocAncestorPermissions + VINCREMENT;
                Attrs[] newAncestorPermissions = new Attrs[nNewAllocAncestors];
                if(vAncestorPermissions != null && nAncestorPermissions > 0)
                    System.arraycopy(vAncestorPermissions, 0, newAncestorPermissions, 0, nAllocAncestorPermissions);
                nAllocAncestorPermissions = nNewAllocAncestors;
                vAncestorPermissions = newAncestorPermissions;
            }
            vAncestorPermissions[nAncestorPermissions++] = attrs;
            nAncestorPermissions++;
        }

        /**
         * append a child string
         * @param child
         */
        protected void appendChild(String child) {
            if (nChildren >= nAllocChildren) {
                int nNewAllocChildren = nAllocChildren + VINCREMENT;
                String[] newChildren = new String[nNewAllocChildren];
                if(vChildren != null && nChildren > 0)
                    System.arraycopy(vChildren, 0, newChildren, 0, nChildren);
                nAllocChildren = nNewAllocChildren;
                vChildren = newChildren;
            }
            vChildren[nChildren] = child;
            nChildren++;
        }

        /**
         * append a file extent
         * @param extent
         */
        protected void appendExtent(Extent extent) {
            if (nExtents >= nAllocExtents) {
                int nNewAlloc = nAllocExtents + VINCREMENT;
                Extent[] newExtents = new Extent[nNewAlloc];
                if(vExtents != null && nExtents > 0)
                    System.arraycopy(vExtents, 0, newExtents, 0, nExtents);
                nAllocExtents = nNewAlloc;
                vExtents = newExtents;
            }
            vExtents[nExtents] = extent;
            nExtents++;
        }


        /**
         * new fs entry
         * @param _name
         * @param _parentpath
         * @param _type
         */
        public FSEntry(
                String _name,
                String _parentpath,
                etype _type,
                int permissions
            ) {
            name = _name;
            parentPath = _parentpath;
            type = _type;
            attrs = new Attrs(permissions, type);
            vChildren = null;
            vExtents = null;
            nChildren = 0;
            nExtents = 0;
            nAncestorPermissions = 0;
            nAllocExtents = 0;
            nAllocChildren = 0;
            nAllocAncestorPermissions = 0;
            nOpenForRead = 0;
            nOpenForWrite = 0;
            nOpenForAppend = 0;
            nRefCount = 0;
        }

        /**
         * new fs entry
         * @param _name
         * @param _parentpath
         * @param _type
         */
        public FSEntry(
                String _name,
                String _parentpath,
                etype _type
            ) {
            this(_name, _parentpath, _type, _ACCESS_DEFAULTS);
        }

        /**
         * random FSEntry generator
         * @param rnd
         * @param _name
         * @param _parentpath
         * @param _type
         * @return
         */
        public static FSEntry
        randomEntry(
                Random rnd,
                String _name,
                String _parentpath,
                etype _type
            ) {
            Attrs attrs = randAttrs(rnd, _type);
            FSEntry entry = new FSEntry(_name, _parentpath, _type, rnd.nextInt(_ACCESS_GLOBAL_EXEC));
            return entry;
        }

        /**
         * return the absolute path
         * @return
         */
        public String path() {
            if(isImplicitRootPath(parentPath)) {
                if(name.compareTo("root") != 0)
                    throw new RuntimeException("malformed fs: multiple roots?");
                return name;
            }
            return getParentPath() + "/" + name;
        }

        /**
         * parent path
         * @return
         */
        public String getParentPath() {
            if(isImplicitRootPath(parentPath)) {
                if(name.compareTo("root") != 0)
                    throw new RuntimeException("malformed fs: multiple roots?");
                return "";
            }
            return parentPath;
        }

        /**
         * return the size
         * applies only to a file FSEntry
         * @return
         */
        public long size() {
            long result = 0;
            if(type == etype.file) {
                for (int i = 0; i < nExtents; i++)
                    result += vExtents[i] != null ? vExtents[i].length : 0;
            }
            return result;
        }

        /**
         * toString
         *
         * @return
         */
        public String toString() {
            switch (type) {
                case dir:
                    return "DIR: " + name + "/[c-cnt:" + numChildren()+ "]";
                case file:
                    return "FILE: " + name + "[" + size() / 1024 + "KB in " + nExtents + " extents]";
            }
            return "";
        }

        /**
         * return something like what "ls -l" would output
         * @return
         */
        public String lsOutput() {
            StringBuilder sb = new StringBuilder();
            sb.append("PATH:\n");
            sb.append("\t");
            sb.append(path());
            sb.append("\n");
            sb.append("CONTENTS:\n");
            sb.append("\ttype:\t\t");
            sb.append(type == etype.dir ? "directory" : "file");
            sb.append("\n");
            sb.append("\tpermissions:\t");
            sb.append(attrs.getAccessString());
            sb.append("\n");
            sb.append("\tanc-perms:\t");
            boolean first = true;
            for(int i=0; i<nAncestorPermissions; i++) {
                if(!first)
                    sb.append("\t\t\t");
                sb.append(vAncestorPermissions[i].getAccessString());
                sb.append("\n");
                first = false;
            }
            sb.append("\tcontents:\t");
            first = true;
            for(int i=0; i< nChildren; i++) {
                if(!first && i % 4 == 0) sb.append("\n\t\t\t");
                sb.append(vChildren[i]);
                sb.append(" ");
                i++;
            }
            sb.append("\n");
            return sb.toString();
        }

        /**
         * add a child entry to dirnode
         *
         * @param child
         */
        public void addChild(FSEntry child) {
            if (type != etype.dir)
                throw new RuntimeException("Invalid operation!");
            appendChild(child.name);
        }

        /**
         * remove a child
         *
         * @param child
         * @return
         */
        public boolean removeChild(String child) {
            if (type != etype.dir)
                throw new RuntimeException("Invalid operation!");
            if (numChildren() <= 0)
                throw new RuntimeException("Invalid operation on empty directory!!");
            ArrayList<String> survivors = new ArrayList<String>();
            boolean found = false;
            int j=0;
            for (int i=0; i<numChildren(); i++) {
                String s = vChildren[i];
                if (s.compareTo(child) == 0) {
                    found = true;
                } else {
                    vChildren[j++] = s;
                }
            }
            nChildren = j;
            return found;
        }

        /**
         * add a new extent to a file node
         *
         * @param logOffset
         * @param phyOffset
         * @param length
         */
        public void addExtent(long logOffset, long phyOffset, long length) {
            if (type != etype.dir)
                throw new RuntimeException("Invalid operation!");
            appendExtent(new Extent(logOffset, phyOffset, length));
        }

        /**
         * add new extents until the
         * file size matches the target length
         * @param newlen
         */
        public void addExtents(long newlen) {
            Random random = new Random();
            while(size() < newlen) {
                long delta = newlen - size();
                long eSize = delta % MAX_EXTENT;
                Extent extent = new Extent(random.nextLong(), 0, eSize);
                appendExtent(extent);
            }
        }

        /**
         * remove/truncate extents until
         * the file size meets the spec.
         * @param newlen
         */
        public void compactExtents(long newlen) {
            if(newlen == 0) {
                // optimize a common case
                nExtents = 0;
                for(int i=0; i<nAllocExtents; i++)
                    vExtents[i] = null;
                return;
            }
            while(size() > newlen) {
                long delta = size() - newlen;
                Extent tail = vExtents[nExtents-1];
                if(delta > tail.length) {
                    vExtents[nExtents-1] = null;
                    nExtents--;
                } else {
                    tail.length -= delta;
                }
            }
        }

        /**
         * simulate open
         * @param perms
         */
        public void open(int perms) {
            nOpenForRead += isRead(perms) ? 1 : 0;
            nOpenForWrite += isWrite(perms) ? 1 : 0;
            nOpenForAppend += isExec(perms) ? 1 : 0;
        }

        /**
         * simulate close
         * @param perms
         */
        public void close(int perms) {
            nOpenForRead -= isRead(perms) ? 1 : 0;
            nOpenForWrite -= isWrite(perms) ? 1 : 0;
            nOpenForAppend -= isExec(perms) ? 1 : 0;
            nOpenForRead = Math.max(nOpenForRead, 0);
            nOpenForWrite = Math.max(nOpenForRead, 0);
            nOpenForAppend = Math.max(nOpenForRead, 0);
        }

        protected boolean isRead(int perms) {
            return ((perms & _ACCESS_GLOBAL_READ) |
                    (perms & _ACCESS_GROUP_READ) |
                    (perms & _ACCESS_USER_READ)) != 0;
        }
        protected boolean isWrite(int perms) {
            return ((perms & _ACCESS_GLOBAL_WRITE) |
                    (perms & _ACCESS_GROUP_WRITE) |
                    (perms & _ACCESS_USER_WRITE)) != 0;
        }
        protected boolean isExec(int perms) {
            return ((perms & _ACCESS_GLOBAL_EXEC) |
                    (perms & _ACCESS_GROUP_EXEC) |
                    (perms & _ACCESS_USER_EXEC)) != 0;
        }
    }

    /**
     * create a new BTree
     * @param tTR
     * @param tsf
     * @param strBTreeClass
     * @return
     */
    protected <T> CDBAbstractBTree<String, T>
    createBTree(
        AbstractRuntime tTR,
        IStreamFactory tsf,
        String strBTreeClass,
        long toid
        ) {
        if(toid == CorfuDBObject.oidnull)
            toid = DirectoryService.getUniqueID(tsf);
        if (strBTreeClass.compareTo("CDBPhysicalBTree") == 0) {
            return new CDBPhysicalBTree<String, T>(tTR, tsf, toid);
        } else if (strBTreeClass.compareTo("CDBLogicalBTree") == 0) {
            return new CDBLogicalBTree<String, T>(tTR, tsf, toid);
        } else {
            throw new RuntimeException("btree class unknown--cannot create BTreeFS!");
        }
    }

    /**
     * ctor
     * @param tTR
     * @param tsf
     * @param nMinAtom
     * @param nMaxAtom
     * @param nMaxDirEntries
     * @param nMaxHeight
     * @param strBTreeClass
     * @param btreeOID
     */
    public
    BTreeFS(
            AbstractRuntime tTR,
            IStreamFactory tsf,
            int nMinAtom,
            int nMaxAtom,
            int nMaxDirEntries,
            int nMaxHeight,
            String strBTreeClass,
            long btreeOID,
            boolean transactional
        ) {
        m_rt = tTR;
        m_sf = tsf;
        m_btree = createBTree(tTR, tsf, strBTreeClass, btreeOID);
        m_nMinAtom = nMinAtom;
        m_nMaxAtom = nMaxAtom;
        m_nMaxDirEntries = nMaxDirEntries;
        m_nMaxHeight = nMaxHeight;
        m_puts = new AtomicLong(0);
        m_gets = new AtomicLong(0);
        m_removes = new AtomicLong(0);
        m_updates = new AtomicLong(0);
        m_transactional = transactional;
    }

    /**
     * ctor
     */
    public
    BTreeFS(
        AbstractRuntime tTR,
        IStreamFactory tsf,
        String strBTreeClass,
        boolean transactional
        ) {
        this(tTR, tsf,
                DEFAULT_MINATOM,
                DEFAULT_MAXATOM,
                DEFAULT_MAXDIRENTIES,
                DEFAULT_MAX_HEIGHT,
                strBTreeClass,
                CorfuDBObject.oidnull,
                transactional);
    }

    /**
     * ctor
     */
    public
    BTreeFS(
            AbstractRuntime tTR,
            IStreamFactory tsf,
            String strBTreeClass,
            long btreeOID,
            boolean transactional
        ) {
        this(tTR, tsf,
                DEFAULT_MINATOM,
                DEFAULT_MAXATOM,
                DEFAULT_MAXDIRENTIES,
                DEFAULT_MAX_HEIGHT,
                strBTreeClass,
                btreeOID,
                transactional);
    }

    /**
     * get
     * @param key
     * @return
     */
    protected FSEntry get(String key) {
        FSEntry entry = m_btree.get(key);
        m_gets.incrementAndGet();
        return entry;
    }

    /**
     * remove
     * @param key
     * @return
     */
    protected FSEntry remove(String key) {
        FSEntry entry = m_btree.remove(key);
        m_removes.incrementAndGet();
        return entry;
    }

    /**
     * put
     * @param key
     * @param value
     */
    protected void put(String key, FSEntry value) {
        m_btree.put(key, value);
        m_puts.incrementAndGet();
    }

    /**
     * update
     * @param key
     * @param value
     * @return
     */
    protected boolean update(String key, FSEntry value) {
        boolean result = m_btree.update(key, value);
        m_updates.incrementAndGet();
        return result;
    }

    private class PathDepthSort implements Comparator<FSEntry> {
        public int compare(FSEntry a, FSEntry b) {
            String aslashlesspath = (new String(a.path())).replace("/", "");
            String bslashlesspath = (new String(b.path())).replace("/", "");
            int adepth = a.path().length() - aslashlesspath.length();
            int bdepth = b.path().length() - bslashlesspath.length();
            return adepth - bdepth;
        }
    }

    /**
     * randomly choose an FSEntry from somewhere in the tree
     * try to prefer longer paths as a way of mutating the fs tree
     * closer to the leaves than to the root.
     * @param rnd
     * @param type
     * @return
     */
    public FSEntry
    randomSelect(
            Random rnd,
            etype type
        ) {
        ArrayList<FSEntry> children = new ArrayList<FSEntry>();
        FSEntry root = get("root");
        FSEntry result = randomSelect(rnd, root, type, 0, children);
        if(children.size() > 0) {
            Collections.sort(children, new PathDepthSort());
            if(children.size() < 4)
                return children.get(children.size()-1);
            int halfsize = children.size() / 2;
            int idx = (int) (Math.floor(rnd.nextDouble()*halfsize)) + halfsize;
            idx = Math.min(idx, children.size()-1);
            FSEntry oresult = children.get(idx);
            if(result == null || result == root)
                return oresult;
            return oresult.path().length() > result.path().length() ? oresult : result;
        }
        return result == root ? null : result;
    }

    /**
     * randomly choose an FSEntry from somewhere in the tree
     * @param rnd
     * @param parent
     * @param type
     * @return
     */
    protected FSEntry
    randomSelect(
            Random rnd,
            FSEntry parent,
            etype type,
            int height,
            ArrayList<FSEntry> candidates
        ) {

        if (type == etype.dir && parent != null) {
            if (rnd.nextDouble() < (0.08*height))
                return parent;
        }
        ArrayList<FSEntry> dirs = new ArrayList<FSEntry>();
        for (int i = 0; i < parent.numChildren(); i++) {
            String strChild = parent.getChild(i);
            FSEntry child = get(parent.path() + "/" + strChild);
            if(child.type == etype.dir)
                dirs.add(child);
            if (child.type == type) {
                if (rnd.nextDouble() < (0.08*height))
                    return child;
                candidates.add(child);
            }
        }
        int ndirs = 0;
        for(FSEntry dir : dirs) {
            FSEntry candidate = randomSelect(rnd, dir, type, height+1, candidates);
            if (candidate != null)
                return candidate;

        }
        return null;
    }


    /**
     * @return
     */
    public static Attrs
    randAttrs(Random rnd, etype type) {
        return new Attrs(randomDate(rnd),
                         randomDate(rnd),
                         randomDate(rnd),
                         rnd.nextInt(_ACCESS_DIRECTORY),
                         type);
    }

    /**
     * random date
     * @param rnd
     * @return
     */
    static Date
    randomDate(Random rnd) {
        long ms = BEGINNING_OF_TIME + (Math.abs(rnd.nextLong()) % (20L * 365 * 24 * 60 * 60 * 1000));
        return new Date(ms);
    }

    /**
     * root can be expressed a few ways:
     * 1. null parent path
     * 2. "" parent path
     * 3. "root"
     * @param strPath
     * @return
     */
    static boolean isImplicitRootPath(String strPath) {
        return strPath == null || strPath.compareTo("") == 0;
    }

    /**
     * canonicalize root paths
     * @param strParentPath
     * @return
     */
    static String canonicalize(String strParentPath) {
        if(isImplicitRootPath(strParentPath))
            return "root";
        return strParentPath;
    }

    /**
     * create a new dir or file node
     * @param rnd
     * @param parent
     * @param minIdLength
     * @param maxIdLength
     * @param dirProbability
     * @return
     */
    public static FSEntry
    randomChild(
            Random rnd,
            FSEntry parent,
            int minIdLength,
            int maxIdLength,
            double dirProbability
        ) {
        double diceRoll = rnd.nextDouble();
        String fsName = randEntryName(rnd, minIdLength, maxIdLength);
        return FSEntry.randomEntry(rnd, fsName, parent.path(), diceRoll < dirProbability ? etype.dir : etype.file);
    }

    /**
     * return a random name for an FSEntry
     * @param minlength
     * @param maxlength
     * @return random string (all lower case)
     */
    public static String
    randEntryName(
            Random rnd,
            int minlength,
            int maxlength
        ) {
        StringBuilder sb = new StringBuilder();
        int len = (int) ((rnd.nextDouble() * (maxlength-minlength)))+minlength;
        for (int i = 0; i < len; i++) {
            int cindex = (int) Math.floor(Math.random() * 26);
            char character = (char) ('a' + cindex);
            sb.append(character);
        }
        return (sb.toString());
    }

    /**
     * create an empty FS
     * @param tTR
     * @param tsf
     * @param strBTreeClass
     * @return
     */
    public static BTreeFS
    createEmptyFS(
            AbstractRuntime tTR,
            IStreamFactory tsf,
            String strBTreeClass,
            boolean transactional
        ) {
        BTreeFS fs = new BTreeFS(tTR, tsf, strBTreeClass, transactional);
        FSEntry root = new FSEntry("root", null, etype.dir);
        fs.put(root.path(), root);
        return fs;
    }

    /**
     * create an empty FS
     * @param tTR
     * @param tsf
     * @param strBTreeClass
     * @return
     */
    public static BTreeFS
    attachFS(
            AbstractRuntime tTR,
            IStreamFactory tsf,
            String strBTreeClass,
            long btreeOID,
            boolean transactional
        ) {
        return new BTreeFS(tTR, tsf, strBTreeClass, btreeOID, transactional);
    }


    /**
     * populate a random fs
     * @param tTR
     * @param tsf
     * @param strBTreeClass
     * @param minIdLength
     * @param maxIdLength
     * @param maxChildren
     * @param dirProbability
     * @param height
     * @return
     */
    public static BTreeFS
    createRandomFS(
            AbstractRuntime tTR,
            IStreamFactory tsf,
            String strBTreeClass,
            int minIdLength,
            int maxIdLength,
            int minChildren,
            int maxChildren,
            double dirProbability,
            int height,
            boolean transactional,
            Collection<FileSystemDriver.Op> ops
        ) {
        BTreeFS fs = new BTreeFS(tTR, tsf, strBTreeClass, transactional);
        Random rnd = new Random();
        FSEntry root = new FSEntry("root", null, etype.dir);
        fs.put(root.path(), root);
        fs.populateRandomFS(rnd, root, minIdLength, maxIdLength, minChildren, maxChildren, dirProbability, height-1, ops);
        int actualHeight = fs.depth();
        if(actualHeight < height) {
            System.out.format("Failed to generate a tree of the required height (wanted %d, got %d)\n", height, actualHeight);
            System.out.print(fs.printFS());
            return null;
        }
        return fs;
    }

    /**
     * given a parent directory, populate it.
     * @param rnd
     * @param parent
     * @param minIdLength
     * @param maxIdLength
     * @param maxChildren
     * @param dirProbability
     * @param height
     */
    protected void
    populateRandomFS(
            Random rnd,
            FSEntry parent,
            int minIdLength,
            int maxIdLength,
            int minChildren,
            int maxChildren,
            double dirProbability,
            int height,
            Collection<FileSystemDriver.Op> ops
        )
    {
        boolean leaf = (height <= 0);
        int nChildren = rnd.nextInt(maxChildren-minChildren)+minChildren;
        HashSet<String> siblings = new HashSet<>();
        int nChildDirectories = 0;

        for(int i=0; i<nChildren; i++) {

            // unless we are at the bottom level of the tree, we require
            // at least one directory entry. Technically, all we really need
            // to satisfy the height requirements is a single branch of the
            // required depth, but a more balanced FS tree is likely to provide
            // more easily understood behavior from the runtime anyway. In
            // short, unless we are at the bottom-most level of the tree,
            // adjust the probability of a directory dynamically to ensure
            // we get at least one for non-leaf nodes, and 0 for leaves.

            double nextDirProbability = leaf ?
                    0.0 : (nChildDirectories == 0 && i == nChildren-1) ?
                    1.0 : dirProbability;

            // avoid duplicate names for siblings.
            // a real FS of course should support this if
            // one is a directory and the other a file, but
            // handling such corner cases is not super-important
            // in this context (where we just want a workload to drive CorfuDB}
            FSEntry child = randomChild(rnd, parent, minIdLength, maxIdLength, nextDirProbability);
            while(siblings.contains(child.name)) {
                long unq = System.currentTimeMillis() % 83;
                child.name = child.name + unq;
            }
            siblings.add(child.name);

            if(child.type == etype.dir) {
                assert(height > 0);
                nChildDirectories++;
                ops.add(FileSystemDriver.newMkdirOp(child.name,
                                                    parent.path(),
                                                    new Integer(child.attrs.permissions)));
                setAncestorPermissions(child);
                put(child.path(), child);
                populateRandomFS(rnd, child, minIdLength, maxIdLength,
                        minChildren, maxChildren, dirProbability, height - 1, ops);
                put(child.path(), child);

            } else {

                ops.add(FileSystemDriver.newCreateOp(child.name,
                                                     parent.path(),
                                                     new Integer(child.attrs.permissions)));
                setAncestorPermissions(child);
                put(child.path(), child);
            }
            parent.addChild(child);
            update(parent.path(), parent);
        }
    }

    /**
     * rename
     * @param path
     * @param newName
     * @return
     */
    public boolean
    rename(String path, String newName) {
        boolean inTX = false;
        boolean done = false;
        boolean result = false;
        while(!done) {
            try {
                inTX = BeginTX();
                FSEntry entry = get(path);
                result = rename(entry, newName);
                done = EndTX();
            } catch(Exception e) {
                inTX = AbortTX(inTX, e);
                if(e.getMessage().startsWith("unimplemented!"))
                    throw e;
            }
        }
        return result;
    }

    /**
     * rename
     * @param entry
     * @param newName
     * @return
     */
    public boolean
    rename(FSEntry entry, String newName) {
        throw new RuntimeException("unimplemented!");
    }


    /**
     * open file
     * @param path
     * @return
     */
    public FSEntry
    open(String path, int mode) {

        boolean inTX = false;
        boolean done = false;
        FSEntry file = null;
        while(!done) {
            try {
                inTX = BeginTX();
                file = get(path);
                if(file != null) {
                    // TODO: implement access checks
                    file.open(mode);
                    put(file.path(), file);
                }
                done = EndTX();
                inTX = false;
            } catch (Exception e) {
                inTX = AbortTX(inTX, e);
            }
        }
        return file;
    }

    /**
     * access
     * @param path
     * @return
     */
    public int
    access(String path, int mode) {

        int actualmode = 0;
        boolean inTX = false;
        boolean done = false;
        FSEntry file = null;
        while(!done) {
            try {
                inTX = BeginTX();
                file = get(path);
                if(file != null) {
                    actualmode = mode & file.attrs.permissions;
                    String parentpath = file.getParentPath();
                    while(parentpath.compareTo("root") != 0) {
                        FSEntry parent = get(parentpath);
                        if(parent == null)
                            break;
                        actualmode &= parent.attrs.permissions;
                        parentpath = parent.getParentPath();
                    }
                }
                done = EndTX();
                inTX = false;
            } catch (Exception e) {
                inTX = AbortTX(inTX, e);
            }
        }
        return actualmode;
    }

    /**
     * close file
     * @param entry
     * @return
     */
    public void
    close(FSEntry entry) {
        if(entry == null) return;
        boolean inTX = false;
        boolean done = false;
        FSEntry file = null;
        while(!done) {
            try {
                inTX = BeginTX();
                entry.close(Integer.MAX_VALUE);
                put(entry.path(), entry);
                done = EndTX();
                inTX = false;
            } catch (Exception e) {
                inTX = AbortTX(inTX, e);
            }
        }
    }

    /**
     * emulate a read file system
     * call--since this class is about *metadata*
     * management, a read system call does nothing.
     * Just return an error if the entry is invalid
     * @param file
     * @param buf
     * @param count
     * @return
     */
    public int
    read(FSEntry file,
         byte[] buf,
         int count
        ){
        if(file != null)
            return Math.min(buf.length, count);
        return 0;
    }

    /**
     * write system call.
     * Again, we're just managing file system
     * metadata here, so this really just comes
     * down to checking the write against the current
     * file size--ultimately, under this design, it's just
     * a read on the the meta data, even though there would
     * be writes for extents
     * @param file
     * @param buf
     * @param count
     * @return
     */
    public int
    write(FSEntry file,
          int fOffset,
          byte[] buf,
          int bufOffset,
          int count
        ) {
        int result = 0;
        boolean inTX = false;
        boolean done = false;
        while(!done) {
            try {
                inTX = BeginTX();
                result = _write(file, fOffset, buf, bufOffset, count);
                done = EndTX();
                inTX = false;
            } catch (Exception e) {
                inTX = AbortTX(inTX, e);
            }
        }
        return result;
    }

    /**
     * write system call.
     * Again, we're just managing file system
     * metadata here, so this really just comes
     * down to a potential change in the length of
     * the file.
     * @param file
     * @param buf
     * @param count
     * @return
     */
    protected int
    _write(FSEntry file,
           int fOffset,
           byte[] buf,
           int bufOffset,
           int count
        ) {
        if(file == null || buf == null || count == 0)
            return 0;
        if(buf.length - bufOffset > count)
            return 0;
        if(file.size() - fOffset > count)
            return 0;
        FSEntry entry = get(file.path());
        if(entry == null)
            return 0;
        return count;
    }

    /**
     * trunc system call.
     * Again, we're just managing file system
     * metadata here, so this really just comes
     * down to a potential change in the length of
     * the file.
     * @param file
     * @param newlen
     * @return
     */
    public long
    resize(FSEntry file,
           long newlen) {
        long result = 0;
        boolean inTX = false;
        boolean done = false;
        while(!done) {
            try {
                inTX = BeginTX();
                result = _resize(file, newlen);
                done = EndTX();
                inTX = false;
            } catch (Exception e) {
                inTX = AbortTX(inTX, e);
            }
        }
        return result;
    }

    /**
     * trunc system call.
     * Again, we're just managing file system
     * metadata here, so this really just comes
     * down to a potential change in the length of
     * the file.
     * @param file
     * @param newlen
     * @return
     */
    protected long
    _resize(FSEntry file, long newlen) {

        // TODO: permissions checks!
        if(file == null || newlen < 0 || file.type != etype.file)
            return 0;
        long cursize = file.size();
        if(newlen == cursize)
            return newlen;
        if(newlen > cursize) {
            file.addExtents(newlen);
        } else {
            file.compactExtents(newlen);
        }
        put(file.path(), file);
        return newlen;
    }

    /**
     * @return max depth of file system
     */
    public int depth() {

        int nDepth = 0;
        boolean inTX = false;
        boolean done = false;
        while(!done) {
            try {
                inTX = BeginTX();
                FSEntry root = get("root");
                nDepth = depth(root);
                done = EndTX();
                inTX = false;
            } catch (Exception e) {
                inTX = AbortTX(inTX, e);
            }
        }
        return nDepth;
    }

    /**
     * search for matching files
     * @return
     */
    protected int depth(FSEntry fs) {
        int nDepth = 0;
        if(fs != null) {
            nDepth = 1;
            if (fs.type != etype.file) {
                int nChildDepth = 0;
                for (int i = 0; i < fs.numChildren(); i++) {
                    String path = fs.path() + "/" + fs.getChild(i);
                    FSEntry child = get(path);
                    nChildDepth = Math.max(nChildDepth, depth(child));
                }
                nDepth += nChildDepth;
            }
        }
        return nDepth;
    }

    /**
     * search for a file or directory in the given directory
     * @param parent
     * @param name
     * @return
     */
    public List<FSEntry>
    search(String parent,
           String name) {

        // TODO: access checks!
        ArrayList<FSEntry> matches = null;
        boolean inTX = false;
        boolean done = false;
        while(!done) {
            try {
                inTX = BeginTX();
                String strCanonicalPath = canonicalize(parent);
                FSEntry root = get(strCanonicalPath);
                matches = search(root, name);
                done = EndTX();
                inTX = false;
            } catch (Exception e) {
                inTX = AbortTX(inTX, e);
            }
        }
        return matches;
    }

    /**
     * search for matching files
     * @return
     */
    protected ArrayList<FSEntry>
    search(
            FSEntry fs,
            String fname
        ) {
        ArrayList<FSEntry> matches = new ArrayList<FSEntry>();
        if(fs != null) {
            if (fs.type == etype.file) {
                if (fs.name.compareTo(fname) == 0)
                    matches.add(fs);
            } else {
                for (int i = 0; i < fs.numChildren(); i++) {
                    String path = fs.path() + "/" + fs.getChild(i);
                    FSEntry child = get(path);
                    if(child == null)
                        continue;   // this is a legit case: delete just marks entries
                    if (child.name.compareTo(fname) == 0)
                        matches.add(child);
                    matches.addAll(search(child, fname));
                }
            }
        }
        return matches;
    }

    /**
     * delete a file
     * @param path
     * @return
     */
    public boolean
    delete(String path) {

        boolean inTX = false;
        boolean done = false;
        boolean result = false;
        while(!done) {
            try {
                inTX = BeginTX();
                FSEntry entry = get(path);
                result = delete(entry);
                done = EndTX();
                inTX = false;
            } catch (Exception e) {
                inTX = AbortTX(inTX, e);
            }
        }
        return result;
    }

    /**
     * delete a file
     * @param file
     * @return
     */
    protected boolean
    delete(FSEntry file) {
        // TODO: access checks
        boolean result = false;
        if (file != null && file.type == etype.file) {
            FSEntry parent = get(file.getParentPath());
            if(parent != null) {
                result = remove(file.path()) != null;
                result &= parent.removeChild(file.name);
            }
        }
        return result;
    }

    /**
     * delete a directory
     * @param path
     * @return
     */
    public boolean
    rmdir(String path) {

        boolean inTX = false;
        boolean done = false;
        boolean result = false;
        while(!done) {
            try {
                inTX = BeginTX();
                FSEntry dir = get(path);
                result = rmdir(dir);
                done = EndTX();
                inTX = false;
            } catch (Exception e) {
                inTX = AbortTX(inTX, e);
            }
        }
        return result;
    }

    /**
     * delete a directory
     * @param entry
     * @return
     */
    protected boolean
    rmdir(FSEntry entry) {

        //  TODO: access checks
        boolean result = false;
        if (entry != null && entry.type == etype.dir) {
            String parentPath = entry.getParentPath();
            if(isImplicitRootPath(parentPath)) {
                if(entry.path().compareTo("root") != 0)
                    throw new RuntimeException("malformed fs: multiple roots?");
                return false; // refuse to remove root.
            }
            for (int i = 0; i < entry.numChildren(); i++) {
                FSEntry child = get(entry.path() + "/" + entry.getChild(i));
                if(child != null) { // null-ok: the numChildren includes potentially deleted nodes!
                    if (child.type == etype.dir)
                        result &= rmdir(child);
                    else
                        result &= delete(child);
                }
            }
            FSEntry parent = get(parentPath);
            result = remove(entry.path()) != null;
            if (parent != null)
                result &= parent.removeChild(entry.name);
        }
        return result;
    }

    /**
     * create a new file
     * @param fsName
     * @param parentPath
     * @param permissions
     * @return
     */
    public boolean
    create(
        String fsName,
        String parentPath,
        int permissions
        ) {
        boolean result = false;
        boolean inTX = false;
        boolean done = false;
        while(!done) {
            try {
                inTX = BeginTX();
                String canonicalParentPath = canonicalize(parentPath);
                FSEntry parent = get(canonicalParentPath);
                FSEntry existingEntry = get(canonicalParentPath + "/" + fsName);
                if(parent != null &&                // parent exists?
                   parent.type == etype.dir &&      // parent is actually a directory?
                   existingEntry == null) {         // conflicting entry already exists?
                    FSEntry file = new FSEntry(fsName, parent.path(), etype.file, permissions);
                    setAncestorPermissions(file);
                    parent.addChild(file);
                    put(file.path(), file);
                    update(parent.path(), parent);
                    result = true;
                }
                done = EndTX();
                inTX = false;
            } catch (Exception e) {
                inTX = AbortTX(inTX, e);
            }
        }
        return result;
    }

    /**
     * create a new directory
     * @param fsName
     * @param parentPath
     * @param permissions
     * @return
     */
    public boolean
    mkdir(
            String fsName,
            String parentPath,
            int permissions
        ) {
        boolean result = false;
        boolean inTX = false;
        boolean done = false;
        while(!done) {
            try {
                inTX = BeginTX();
                String strCanonicalPath = canonicalize(parentPath);
                FSEntry parent = get(strCanonicalPath);
                result = mkdir(fsName, parent, permissions);
                done = EndTX();
                inTX = false;
            } catch (Exception e) {
                inTX = AbortTX(inTX, e);
            }
        }
        return result;
    }

    /**
     * create a new directory
     * @param fsName
     * @param parent
     * @param permissions
     * @return
     */
    protected boolean
    mkdir(String fsName,
          FSEntry parent,
          int permissions) {

        // TODO: access checks
        if(parent == null || parent.type != etype.dir)
            return false;
        FSEntry existingDir = get(parent.path() + "/" + fsName);
        if(existingDir == null) {
            FSEntry dir = new FSEntry(fsName, parent.path(), etype.dir, permissions);
            setAncestorPermissions(dir);
            parent.addChild(dir);
            put(dir.path(), dir);
            update(parent.path(), parent);
            return true;
        }
        return false;
    }

    /**
     * set the ancestor permissions for
     * a given entry
     * @param entry
     */
    protected void
    setAncestorPermissions(FSEntry entry) {
        String parentPath = entry.getParentPath();
        FSEntry parent = isImplicitRootPath(parentPath) ? null : get(parentPath);
        while(parent != null) {
            entry.addAncestorPermissions(parent.attrs);
            parentPath = parent.getParentPath();
            parent = isImplicitRootPath(parentPath) ? null : get(parentPath);
        }
    }

    /**
     * read dir
     * @param strPath
     * @return
     */
    public FSEntry[]
    readdir(String strPath) {
        FSEntry[] result = null;
        boolean inTX = false;
        boolean done = false;
        while(!done) {
            try {
                inTX = BeginTX();
                String strCanonicalPath = canonicalize(strPath);
                FSEntry parent = get(strCanonicalPath);
                result = readdir(parent);
                done = EndTX();
                inTX = false;
            } catch (Exception e) {
                inTX = AbortTX(inTX, e);
            }
        }
        return result;
    }

    /**
     * read dir
     * @param parent
     * @return
     */
    public FSEntry[]
    readdir(FSEntry parent) {
        // TODO: access checks
        FSEntry[] result = null;
        if(parent != null && parent.type == etype.dir) {
            result = new FSEntry[parent.numChildren()];
            for(int i=0; i<parent.numChildren(); i++) {
                String childPath = parent.path() +"/"+parent.getChild(i);
                result[i] = get(childPath);
            }
        }
        return result;
    }

    /**
     * print the b-tree (not the file system tree)
     * @return
     */
    public String printBTree() { return m_btree.print(); }

    /**
     * print the file system tree
     * @return
     */
    public String printFS() {
        FSEntry root = get("root");
        return printFS(root, "");
    }

    /**
     * print the file system tree helper
     * @return
     */
    protected String printFS(FSEntry fs, String indent) {
        if(fs == null) return "";
        StringBuilder sb = new StringBuilder();
        sb.append(indent);
        if(fs.type == etype.file) {
            sb.append("*");
            sb.append(fs.name);
            sb.append("\t");
            sb.append(fs.attrs.getAccessString());
            sb.append("\n");
        } else {
            sb.append(fs.name);
            sb.append("\t");
            sb.append(fs.attrs.getAccessString());
            sb.append("/\n");
            for(int i=0; i<fs.numChildren(); i++) {
                String path = fs.path() + "/" + fs.getChild(i);
                FSEntry child = get(path);
                if(child != null) {
                    sb.append(printFS(child, indent + "  "));
                }
            }
        }
        return sb.toString();
    }

    /**
     * helper function for begintx
     * cleans up some of the retry logic
     * @return
     */
    protected boolean BeginTX() {
        if(m_rt != null && m_transactional) {
            m_rt.BeginTX();
            return true;
        }
        return false;
    }

    /**
     * helper function for end tx
     * cleans up some of the retry logic
     * @return
     */
    protected boolean EndTX() {
        if(m_rt != null && m_transactional)
            return m_rt.EndTX();
        return true;
    }

    /**
     * helper function for AbortTX
     * cleans up some of the retry logic
     * @return
     */
    protected boolean AbortTX(boolean inTX, Exception e) {
        boolean retry = true;
        if(m_rt != null && m_transactional) {
            if(inTX)
                m_rt.AbortTX();
            // retry only if the exception could be the
            // result of an inconsistent view, which is only
            // the case in the presence of concurrency. Generally,
            // we must assume this, but if the programmer (ie debugger!)
            // knows there is no concurrency, it's worth avoiding
            // infinite loops for program bugs executing in txns.
            retry = !CDBAbstractBTree.s_singleThreadOptimization;
        }
        System.out.format("%s: %s\n", (retry?"Abort/Retry":"Abort/Fail"), e.getMessage());
        e.printStackTrace();
        if(!retry)
            throw new RuntimeException(e);
        return false;
    }


    /**
     * basic fs populate/enumerate test
     */
    public static void
    fstestBasic(
            AbstractRuntime tTR,
            IStreamFactory tsf,
            String strBTreeClass,
            boolean transactional
        ) {
        double dirProbability = 0.4;
        int minChildren = 2;
        int maxChildren = 10;
        int minIdLength = 1;
        int maxIdLength = 8;
        int height = 5;
        fstestBasic(tTR, tsf, strBTreeClass, dirProbability, minChildren, maxChildren,
                minIdLength, maxIdLength, height, transactional);
    }

    /**
     * basic fs populate/enumerate test
     */
    public static void
    fstestBasic(
            AbstractRuntime tTR,
            IStreamFactory tsf,
            String strBTreeClass,
            double dirProbability,
            int minChildren,
            int maxChildren,
            int minIdLength,
            int maxIdLength,
            int height,
            boolean transactional
        ) {
        ArrayList<FileSystemDriver.Op> initOps = new ArrayList();
        BTreeFS fs = BTreeFS.createRandomFS(tTR, tsf, strBTreeClass,
                minIdLength, maxIdLength, minChildren, maxChildren, dirProbability, height, transactional, initOps);
        System.out.println("FS-tree:\n"+fs.printBTree());
        System.out.println("FS:\n"+fs.printFS());
    }

    /**
     * synthetic fs populate/enumerate/mutate test
     */
    public static void
    fstestRecord(
            AbstractRuntime tTR,
            IStreamFactory tsf,
            String strBTreeClass,
            boolean transactional,
            int nTargetFSDepth,
            int nTargetFSFanout,
            int nWorkloadOps,
            String initPath,
            String wkldPath
        ) {
        double dirProbability = 0.4;
        int minChildren = 3;
        int minIdLength = 1;
        int maxIdLength = 8;
        int maxChildren = Math.max(nTargetFSFanout, minChildren);
        long curTime = System.currentTimeMillis();
        int nRandomSeed = (int) curTime;
        if(initPath == null) initPath = "initfs_" + (curTime%100) + ".ser";
        if(wkldPath == null) wkldPath = "wkldfs_" + (curTime%100) + ".ser";
        fstestRecord(tTR, tsf, strBTreeClass, dirProbability, minChildren, maxChildren, minIdLength, maxIdLength,
                nTargetFSDepth, nWorkloadOps, nRandomSeed, transactional, initPath, wkldPath);
    }

    /**
     * synthetic fs populate/enumerate/mutate test
     */
    public static void
    fstestRecord(
            AbstractRuntime tTR,
            IStreamFactory tsf,
            String strBTreeClass,
            double dirProbability,
            int minChildren,
            int maxChildren,
            int minIdLength,
            int maxIdLength,
            int height,
            int nWorkloadOperations,
            int nRandomSeed,
            boolean transactional,
            String initPath,
            String wkldPath
        ) {
        ArrayList<FileSystemDriver.Op> initOps = new ArrayList();
        BTreeFS fs = BTreeFS.createRandomFS(tTR, tsf, strBTreeClass,
                minIdLength, maxIdLength, minChildren, maxChildren, dirProbability, height, transactional, initOps);
        if(fs == null) {
            System.out.println("failed to generate a random FS tree of the required dimensions (too small?)");
            System.out.println("FAILED.");
            System.exit(-1);
        }
        String initFSstate = fs.printFS();
        System.out.println("FS-tree:\n"+fs.printBTree());
        System.out.println("FS:\n"+initFSstate);
        FileSystemDriver driver = new FileSystemDriver(fs, nWorkloadOperations);
        if(!driver.SynthesizeWorkload(nRandomSeed)) {
            System.out.println("failed to synthesize a random workoad over the given BTreeFS (too small?)");
            System.out.println("FAILED.");
            System.exit(-1);
        }
        System.out.format("test case:\n%s\n", driver.toString());
        driver.play();
        driver.setInitOps(initOps);
        String wkldFSstate = fs.printFS();
        System.out.format("test case (with init):\n%s\n", driver.toString());
        System.out.println("FS after test:\n" + wkldFSstate);
        driver.Persist(initPath, wkldPath, initFSstate, wkldFSstate);
    }

    /**
     * synthetic fs populate/enumerate/mutate test
     */
    public static void
    fstestPlayback(
            AbstractRuntime tTR,
            IStreamFactory tsf,
            String strBTreeClass,
            String strInitPath,
            String strWkldPath,
            boolean transactional
        ) {
        BTreeFS fs = BTreeFS.createEmptyFS(tTR, tsf, strBTreeClass, transactional);
        FileSystemDriver driver = new FileSystemDriver(fs, strInitPath, strWkldPath);
        System.out.format("test case:\n%s\n", driver.toString());
        driver.playPhase(0);
        String initFSstate = fs.printFS();
        driver.playPhase(1);
        String wkldFSstate = fs.printFS();
        System.out.println("FS after test:\n" + wkldFSstate);
        FileSystemDriver.PersistFSSnapshot(strInitPath + ".post", initFSstate);
        FileSystemDriver.PersistFSSnapshot(strWkldPath + ".post", wkldFSstate);
    }

    static final String strBTREEOID = "BTREEOID: ";
    static final String strINDEXOID = "INDEXOID: ";

    /**
     * synthetic fs populate/enumerate/mutate test
     */
    public static void
    fstestCrash(
            AbstractRuntime tTR,
            IStreamFactory tsf,
            String strBTreeClass,
            boolean transactional,
            String strInitPath,
            String strWkldPath,
            int nCrashOp
        ) {
        BTreeFS fs = BTreeFS.createEmptyFS(tTR, tsf, strBTreeClass, transactional);
        FileSystemDriver driver = new FileSystemDriver(fs, strInitPath, strWkldPath);
        System.out.format("test case:\n%s\n", driver.toString());
        driver.init();
        String initFSstate = fs.printFS();
        System.out.println("FS after init:\n" + initFSstate);
        driver.playTo(nCrashOp, false);
        String wkldFSstate = fs.printFS();
        System.out.println("FS after test:\n" + wkldFSstate);
        System.out.println("BTree REQ stats:\n" + fs.m_btree.getLatencyStats());
        System.out.println(strBTREEOID + fs.m_btree.oid);
        FileSystemDriver.PersistFSSnapshot(strInitPath + ".post", initFSstate);
        FileSystemDriver.PersistFSSnapshot(strWkldPath + ".post-crash", wkldFSstate);
    }

    /**
     * synthetic fs populate/enumerate/mutate test
     */
    public static void
    fstestRecover(
            AbstractRuntime tTR,
            IStreamFactory tsf,
            String strBTreeClass,
            boolean transactional,
            String strInitPath,
            String strWkldPath,
            String strCrashLogPath,
            int nRecoverOp
        ) {
        Pair<Long, Long> oids = recoverOIDs(strCrashLogPath);
        long btreeOID = oids.first;
        System.out.format("Recovering BTreeFS btreeOID:%d\n", btreeOID);
        BTreeFS fs = BTreeFS.attachFS(tTR, tsf, strBTreeClass, btreeOID, transactional);
        FileSystemDriver driver = new FileSystemDriver(fs, strInitPath, strWkldPath);
        System.out.format("test case:\n%s\n", driver.toString());
        driver.playFrom(nRecoverOp);
        String wkldFSstate = fs.printFS();
        System.out.println("FS after test:\n" + wkldFSstate);
        System.out.println("BTree REQ stats:\n" + fs.m_btree.getLatencyStats());
        FileSystemDriver.PersistFSSnapshot(strWkldPath + ".post-recover", wkldFSstate);
    }

    /**
     * get the btree fs root oids
     * from the given text file.
     * @param strPath
     * @return
     */
    public static Pair<Long, Long>
    recoverOIDs(String strPath) {
        try {
            long btreeOID = CorfuDBObject.oidnull;
            long indexOID = CorfuDBObject.oidnull;
            Path path = Paths.get(strPath);
            List<String> lines = Files.readAllLines(path);
            for(String s : lines) {
                if(s.startsWith(strBTREEOID)) {
                    String strOID = s.substring(strBTREEOID.length());
                    btreeOID = Long.parseLong(strOID);
                } else if(s.startsWith(strINDEXOID)) {
                    String strOID = s.substring(strINDEXOID.length());
                    indexOID = Long.parseLong(strOID);
                }
            }
            return new Pair(btreeOID, indexOID);
        } catch(IOException io) {
            System.out.println("failed to find persisted OIDs at " + strPath);
            return null;
        }
    }
}

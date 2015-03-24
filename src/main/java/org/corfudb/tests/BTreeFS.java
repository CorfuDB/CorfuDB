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
    public static final long MAX_EXTENT = 4096;

    protected CDBAbstractBTree<String, FSEntry> m_btree;
    protected CDBAbstractBTree<String, Integer> m_refcnt;
    protected AbstractRuntime m_rt;
    protected StreamFactory m_sf;
    protected int m_nMinAtom;
    protected int m_nMaxAtom;
    protected int m_nMaxDirEntries;
    protected int m_nMaxHeight;
    public AtomicLong m_puts;
    public AtomicLong m_gets;
    public AtomicLong m_removes;
    public AtomicLong m_updates;

    public static class FSAttrs implements Serializable {

        public Date access;
        public Date create;
        public Date modified;
        public long permissions;
        public long size;

        public FSAttrs(long p, long s) {
            access = new Date(BEGINNING_OF_TIME);
            create = new Date(BEGINNING_OF_TIME);
            modified = new Date(BEGINNING_OF_TIME);
            permissions = p;
            size = s;
        }


        public FSAttrs(Date a, Date c, Date m, long p, long s) {
            access = a;
            create = c;
            modified = m;
            permissions = p;
            size = s;
        }
    }

    public static class Extent implements Serializable {
        public long logaddr;
        public long phyaddr;
        public long len;
        public Extent(Random rnd) {
            logaddr = rnd.nextLong();
            phyaddr = rnd.nextLong();
            len = (long) (rnd.nextDouble() * MAX_EXTENT);
        }
        public Extent(long l, long p, long _len) {
            logaddr = l;
            phyaddr = p;
            len = _len;
        }
    }

    public static class FSEntry implements Serializable {

        public static final int VINCREMENT = 8;
        public static final long O_READ = 0x1;
        public static final long O_WRITE = 0x2;
        public static final long O_APPEND = 0x4;
        public FSEntry parent;
        public String name;
        public FSAttrs attrs;
        public etype type;
        private int m_nChildren;
        private int nAllocChildren;
        private int nExtents;
        private int nAllocExtents;
        private String[] vChildren;
        private Extent[] vExtents;
        public int nOpenForRead;
        public int nOpenForWrite;
        public int nOpenForAppend;

        public String getChild(int i) { return i < m_nChildren ? vChildren[i] : null; }
        public void setChild(int i, String s) { vChildren[i] = s; }
        public int numChildren() { return m_nChildren; }

        /**
         * append a child string
         *
         * @param child
         */
        protected void appendChild(String child) {
            if (m_nChildren >= nAllocChildren) {
                int nNewAllocChildren = nAllocChildren + VINCREMENT;
                String[] newChildren = new String[nNewAllocChildren];
                if(vChildren != null && m_nChildren > 0)
                    System.arraycopy(vChildren, 0, newChildren, 0, m_nChildren);
                nAllocChildren = nNewAllocChildren;
                vChildren = newChildren;
            }
            vChildren[m_nChildren] = child;
            m_nChildren++;
        }

        /**
         * append a file extent
         *
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
         *
         * @param _name
         * @param _parent
         * @param _type
         */
        public FSEntry(
                String _name,
                FSEntry _parent,
                etype _type,
                long permissions,
                long size
        ) {
            name = _name;
            parent = _parent;
            type = _type;
            attrs = new FSAttrs(permissions, size);
            vChildren = null;
            vExtents = null;
            m_nChildren = 0;
            nExtents = 0;
            nOpenForRead = 0;
            nOpenForWrite = 0;
            nOpenForAppend = 0;
        }

        /**
         * new fs entry
         *
         * @param rnd
         * @param _name
         * @param _parent
         * @param _type
         */
        public FSEntry(
                Random rnd,
                String _name,
                FSEntry _parent,
                etype _type
        ) {
            name = _name;
            parent = _parent;
            type = _type;
            attrs = BTreeFS.randAttrs(rnd);
            vChildren = null;
            vExtents = null;
            m_nChildren = 0;
            nExtents = 0;
            nOpenForRead = 0;
            nOpenForWrite = 0;
            nOpenForAppend = 0;
        }

        /**
         * return the absolute path
         *
         * @return
         */
        public String path() {
            String ppath = parent == null ? "" : parent.path() + "/";
            return ppath + name;
        }

        /**
         * parent path
         * @return
         */
        public String parentpath() {
            return parent == null ? "" : parent.path();
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
                    return "FILE: " + name + "[" + attrs.size + "KB, blkcnt:" + nExtents + "]";
            }
            return "";
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
            m_nChildren = j;
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
         * simulate open
         *
         * @param perms
         */
        public void open(long perms) {
            nOpenForRead += (perms & O_READ) != 0 ? 1 : 0;
            nOpenForWrite += (perms & O_WRITE) != 0 ? 1 : 0;
            nOpenForAppend += (perms & O_APPEND) != 0 ? 1 : 0;
        }

        /**
         * simulate close
         *
         * @param perms
         */
        public void close(long perms) {
            nOpenForRead -= (perms & O_READ) != 0 ? 1 : 0;
            nOpenForWrite -= (perms & O_WRITE) != 0 ? 1 : 0;
            nOpenForAppend -= (perms & O_APPEND) != 0 ? 1 : 0;
            nOpenForRead = Math.max(nOpenForRead, 0);
            nOpenForWrite = Math.max(nOpenForRead, 0);
            nOpenForAppend = Math.max(nOpenForRead, 0);
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
        StreamFactory tsf,
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
     * @param indexOID
     */
    public
    BTreeFS(
            AbstractRuntime tTR,
            StreamFactory tsf,
            int nMinAtom,
            int nMaxAtom,
            int nMaxDirEntries,
            int nMaxHeight,
            String strBTreeClass,
            long btreeOID,
            long indexOID
        ) {
        m_rt = tTR;
        m_sf = tsf;
        m_btree = createBTree(tTR, tsf, strBTreeClass, btreeOID);
        m_refcnt = createBTree(tTR, tsf, strBTreeClass, indexOID);
        m_nMinAtom = nMinAtom;
        m_nMaxAtom = nMaxAtom;
        m_nMaxDirEntries = nMaxDirEntries;
        m_nMaxHeight = nMaxHeight;
        m_puts = new AtomicLong(0);
        m_gets = new AtomicLong(0);
        m_removes = new AtomicLong(0);
        m_updates = new AtomicLong(0);
    }

    /**
     * ctor
     */
    public
    BTreeFS(
        AbstractRuntime tTR,
        StreamFactory tsf,
        String strBTreeClass
        ) {
        this(tTR, tsf,
                DEFAULT_MINATOM,
                DEFAULT_MAXATOM,
                DEFAULT_MAXDIRENTIES,
                DEFAULT_MAX_HEIGHT,
                strBTreeClass,
                CorfuDBObject.oidnull,
                CorfuDBObject.oidnull);
    }

    /**
     * ctor
     */
    public
    BTreeFS(
            AbstractRuntime tTR,
            StreamFactory tsf,
            String strBTreeClass,
            long btreeOID,
            long indexOID
        ) {
        this(tTR, tsf,
                DEFAULT_MINATOM,
                DEFAULT_MAXATOM,
                DEFAULT_MAXDIRENTIES,
                DEFAULT_MAX_HEIGHT,
                strBTreeClass,
                btreeOID,
                indexOID);
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
     * getrefcnt
     * @param key
     * @return
     */
    protected int refcount(String key) {
        return m_refcnt.get(key);
    }

    /**
     * addref
     * @param key
     * @return
     */
    protected int addref(String key) {
        Integer i = m_refcnt.get(key);
        Integer newCount = i == null ? new Integer(0) : i;
        m_refcnt.put(key, newCount);
        return newCount;
    }

    /**
     * release
     * @param key
     * @return
     */
    protected int release(String key) {
        Integer i = m_refcnt.get(key);
        m_refcnt.put(key, new Integer(--i));
        return i;
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

    private class LengthSort implements Comparator<FSEntry> {
        public int compare(FSEntry a, FSEntry b) {
            return a.path().length() - b.path().length();
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
    randomSelect(Random rnd, etype type) {
        ArrayList<FSEntry> children = new ArrayList<FSEntry>();
        FSEntry root = get("root");
        FSEntry result = randomSelect(rnd, root, type, children);
        if(result == null && children.size() > 0) {
            Collections.sort(children, new LengthSort());
            if(children.size() < 4)
                return children.get(children.size()-1);
            int halfsize = children.size() / 2;
            int idx = (int) (Math.floor(rnd.nextDouble()*halfsize)) + halfsize;
            idx = Math.min(idx, children.size()-1);
            return children.get(idx);
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
            ArrayList<FSEntry> candidates
        ) {

        if (type == etype.dir && parent != null) {
            if (rnd.nextDouble() < 0.08)
                return parent;
        }
        ArrayList<FSEntry> dirs = new ArrayList<FSEntry>();
        for (int i = 0; i < parent.numChildren(); i++) {
            String strChild = parent.getChild(i);
            FSEntry child = get(parent.path() + "/" + strChild);
            if (child.type == type) {
                if (rnd.nextDouble() < 0.08)
                    return child;
                candidates.add(child);
            }
        }
        int ndirs = 0;
        for(FSEntry dir : dirs) {
            FSEntry candidate = randomSelect(rnd, dir, type, candidates);
            if (candidate != null)
                return candidate;

        }
        return null;
    }


    /**
     * @return
     */
    public static FSAttrs
    randAttrs(Random rnd) {
        return new FSAttrs(randomDate(rnd),
                randomDate(rnd),
                randomDate(rnd),
                rnd.nextLong(),
                Math.abs(rnd.nextLong()) % 16384);
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
        return new FSEntry(rnd, fsName, parent, diceRoll < dirProbability ? etype.dir : etype.file);
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
            StreamFactory tsf,
            String strBTreeClass
        ) {
        BTreeFS fs = new BTreeFS(tTR, tsf, strBTreeClass);
        FSEntry root = new FSEntry("root", null, etype.dir, Long.MIN_VALUE, 0);
        fs.put(root.path(), root);
        fs.addref(root.path());
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
            StreamFactory tsf,
            String strBTreeClass,
            long btreeOID,
            long indexOID
        ) {
        return new BTreeFS(tTR, tsf, strBTreeClass, btreeOID, indexOID);
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
            StreamFactory tsf,
            String strBTreeClass,
            int minIdLength,
            int maxIdLength,
            int maxChildren,
            double dirProbability,
            int height,
            Collection<FileSystemDriver.Op> ops
        ) {
        BTreeFS fs = new BTreeFS(tTR, tsf, strBTreeClass);
        Random rnd = new Random();
        FSEntry root = new FSEntry("root", null, etype.dir, Long.MIN_VALUE, 0);
        fs.put(root.path(), root);
        fs.addref(root.path());
        fs.populateRandomFS(rnd, root, minIdLength, maxIdLength, maxChildren, dirProbability, height, ops);
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
            int maxChildren,
            double dirProbability,
            int height,
            Collection<FileSystemDriver.Op> ops
        )
    {
        int nChildren = rnd.nextInt(maxChildren);
        for(int i=0; i<nChildren; i++) {

            double nextDirProbability = height == 0 ? 0.0 : dirProbability;
            FSEntry child = randomChild(rnd, parent, minIdLength, maxIdLength, nextDirProbability);

            if(child.type == etype.dir) {

                assert(height > 0);
                ops.add(FileSystemDriver.newMkdirOp(child.name,
                                                    child.parentpath(),
                                                    new Long(child.attrs.permissions)));
                put(child.path(), child);
                addref(child.path());
                populateRandomFS(rnd, child, minIdLength, maxIdLength, maxChildren, dirProbability, height-1, ops);

            } else {

                ops.add(FileSystemDriver.newCreateOp(child.name,
                                                     child.parentpath(),
                                                     new Long(child.attrs.permissions)));
                put(child.path(), child);
                addref(child.path());
            }
            parent.addChild(child);
            addref(parent.path());
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
    open(String path, long mode) {
        boolean inTX = false;
        boolean done = false;
        FSEntry file = null;
        while(!done) {
            try {
                inTX = BeginTX();
                file = get(path);
                if(file != null) {
                    file.open(mode);
                    put(file.path(), file);
                    addref(file.path());
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
                entry.close(Long.MAX_VALUE);
                put(entry.path(), entry);
                release(entry.path());
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
     * down to a potential change in the length of
     * the file.
     * @param file
     * @param buf
     * @param count
     * @return
     */
    public int
    write(FSEntry file,
          byte[] buf,
          int count) {
        int result = 0;
        boolean inTX = false;
        boolean done = false;
        while(!done) {
            try {
                inTX = BeginTX();
                result = _write(file, buf, count);
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
          byte[] buf,
          int count) {
        if(file == null || buf == null || count == 0)
            return 0;
        file.attrs.size += count; // just a simulation...
        put(file.path(), file);
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
    public int
    trunc(FSEntry file,
          int newlen) {
        int result = 0;
        boolean inTX = false;
        boolean done = false;
        while(!done) {
            try {
                inTX = BeginTX();
                result = _trunc(file, newlen);
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
    protected int
    _trunc(FSEntry file,
          int newlen) {
        if(file == null)
            return 0;
        file.attrs.size = newlen; // just a simulation...
        put(file.path(), file);
        return newlen;
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

        ArrayList<FSEntry> matches = null;
        boolean inTX = false;
        boolean done = false;
        while(!done) {
            try {
                inTX = BeginTX();
                FSEntry root = parent == null ? get("root") : get(parent);
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
        boolean result = false;
        if (file != null && file.type == etype.file) {
            FSEntry parent = get(file.parent.path());
            result = remove(file.path()) != null;
            result &= parent.removeChild(file.name);
            release(file.path());       // decrement refcount
            release(parent.path());     // decrement refcount
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
        boolean result = false;
        if (entry != null && entry.type == etype.dir) {
            if(entry.parent == null)
                return false; // refuse to remove root.
            for(int i=0; i<entry.numChildren(); i++) {
                FSEntry child = get(entry.path() + "/" + entry.getChild(i));
                if(child.type == etype.dir)
                    result &= rmdir(child);
                else
                    result &= delete(child);
            }
            FSEntry parent = entry.parent == null ? null : get(entry.parent.path());
            result = remove(entry.path()) != null;
            release(entry.path());
            if(parent != null) {
                result &= parent.removeChild(entry.name);
                release(parent.path());
            }
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
        long permissions
        ) {
        boolean result = false;
        boolean inTX = false;
        boolean done = false;
        while(!done) {
            try {
                inTX = BeginTX();
                FSEntry parent = get(parentPath == null ? "root" : parentPath);
                FSEntry existingEntry = get(parentPath + "/" + fsName);
                if(parent != null && existingEntry == null) { // parent exists? new entry already exists?
                    FSEntry file = new FSEntry(fsName, parent, etype.file, 0, permissions);
                    parent.addChild(file);
                    put(file.path(), file);
                    addref(file.path());
                    update(parent.path(), parent);
                    addref(parent.path());
                    result = true;
                }
                done = EndTX();
                inTX = false;
            } catch (Exception e) {
                inTX = AbortTX(inTX, e);
            }
        }
        return true;
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
            long permissions
        ) {
        boolean result = false;
        boolean inTX = false;
        boolean done = false;
        while(!done) {
            try {
                inTX = BeginTX();
                FSEntry parent = get(parentPath == null ? "root" : parentPath);
                result = mkdir(fsName, parent, permissions);
                done = EndTX();
                inTX = false;
            } catch (Exception e) {
                inTX = AbortTX(inTX, e);
            }
        }
        return true;
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
          long permissions) {

        if(parent == null || parent.type != etype.dir)
            return false;
        FSEntry existingDir = get(parent.path() + "/" + fsName);
        FSEntry dir = new FSEntry(fsName, parent, etype.dir, 0, permissions);
        if(dir != null && existingDir == null) {
            parent.addChild(dir);
            put(dir.path(), dir);
            update(parent.path(), parent);
            addref(dir.path());
            addref(parent.path());
            return true;
        }
        return false;
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
                FSEntry parent = get(strPath == null ? "root" : strPath);
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
        if(fs == null)
            return "--missing--";
        StringBuilder sb = new StringBuilder();
        sb.append(indent);
        if(fs.type == etype.file) {
            sb.append("*");
            sb.append(fs.name);
            sb.append("\n");
        } else {
            sb.append(fs.name);
            sb.append("/\n");
            for(int i=0; i<fs.numChildren(); i++) {
                String path = fs.path() + "/" + fs.getChild(i);
                FSEntry child = get(path);
                sb.append(printFS(child, indent+"  "));
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
        if(m_rt != null) {
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
        if(m_rt != null)
            return m_rt.EndTX();
        return true;
    }

    /**
     * helper function for AbortTX
     * cleans up some of the retry logic
     * @return
     */
    protected boolean AbortTX(boolean inTX, Exception e) {
        if(m_rt != null) {
            if(inTX)
                m_rt.AbortTX();
            return false;
        }
        throw new RuntimeException(e);
    }


    /**
     * basic fs populate/enumerate test
     */
    public static void
    fstestBasic(
            AbstractRuntime tTR,
            StreamFactory tsf,
            String strBTreeClass
        ) {
        double dirProbability = 0.4;
        int maxChildren = 10;
        int minIdLength = 1;
        int maxIdLength = 8;
        int height = 5;
        fstestBasic(tTR, tsf, strBTreeClass, dirProbability, maxChildren, minIdLength, maxIdLength, height);
    }

    /**
     * basic fs populate/enumerate test
     */
    public static void
    fstestBasic(
            AbstractRuntime tTR,
            StreamFactory tsf,
            String strBTreeClass,
            double dirProbability,
            int maxChildren,
            int minIdLength,
            int maxIdLength,
            int height
        ) {
        ArrayList<FileSystemDriver.Op> initOps = new ArrayList();
        BTreeFS fs = BTreeFS.createRandomFS(tTR, tsf, strBTreeClass,
                minIdLength, maxIdLength, maxChildren, dirProbability, height, initOps);
        System.out.println("FS-tree:\n"+fs.printBTree());
        System.out.println("FS:\n"+fs.printFS());
    }

    /**
     * synthetic fs populate/enumerate/mutate test
     */
    public static void
    fstestSynthetic(
            AbstractRuntime tTR,
            StreamFactory tsf,
            String strBTreeClass
    ) {
        double dirProbability = 0.4;
        int maxChildren = 10;
        int minIdLength = 1;
        int maxIdLength = 8;
        int height = 5;
        fstestSynthetic(tTR, tsf, strBTreeClass, dirProbability, maxChildren, minIdLength, maxIdLength, height);
    }

    /**
     * synthetic fs populate/enumerate/mutate test
     */
    public static void
    fstestSynthetic(
            AbstractRuntime tTR,
            StreamFactory tsf,
            String strBTreeClass,
            double dirProbability,
            int maxChildren,
            int minIdLength,
            int maxIdLength,
            int height
        ) {
        ArrayList<FileSystemDriver.Op> initOps = new ArrayList();
        BTreeFS fs = BTreeFS.createRandomFS(tTR, tsf, strBTreeClass,
                minIdLength, maxIdLength, maxChildren, dirProbability, height, initOps);
        System.out.println("FS-tree:\n"+fs.printBTree());
        System.out.println("FS:\n"+fs.printFS());
        FileSystemDriver driver = new FileSystemDriver(fs, 50, 100);
        System.out.format("test case:\n%s\n", driver.toString());
        driver.play();
        driver.setInitOps(initOps);
        System.out.format("test case (with init):\n%s\n", driver.toString());
        System.out.println("FS after test:\n" + fs.printFS());
        int unq = (int)(System.currentTimeMillis() % 100);
        String initPath = "initfs_" + unq + ".ser";
        String wkldPath = "wkldfs_" + unq + ".ser";
        driver.Persist(initPath, wkldPath);
    }

    /**
     * synthetic fs populate/enumerate/mutate test
     */
    public static void
    fstestSynthetic(
            AbstractRuntime tTR,
            StreamFactory tsf,
            String strBTreeClass,
            String strInitPath,
            String strWkldPath
        ) {
        BTreeFS fs = BTreeFS.createEmptyFS(tTR, tsf, strBTreeClass);
        FileSystemDriver driver = new FileSystemDriver(fs, strInitPath, strWkldPath);
        System.out.format("test case:\n%s\n", driver.toString());
        driver.play();
        System.out.println("FS after test:\n" + fs.printFS());
    }

    static final String strBTREEOID = "BTREEOID: ";
    static final String strINDEXOID = "INDEXOID: ";

    /**
     * synthetic fs populate/enumerate/mutate test
     */
    public static void
    fstestCrash(
            AbstractRuntime tTR,
            StreamFactory tsf,
            String strBTreeClass,
            String strInitPath,
            String strWkldPath,
            int nCrashOp
        ) {
        BTreeFS fs = BTreeFS.createEmptyFS(tTR, tsf, strBTreeClass);
        FileSystemDriver driver = new FileSystemDriver(fs, strInitPath, strWkldPath);
        System.out.format("test case:\n%s\n", driver.toString());
        driver.playTo(nCrashOp);
        System.out.println("FS after test:\n" + fs.printFS());
        System.out.println(strBTREEOID + fs.m_btree.oid);
        System.out.println(strINDEXOID + fs.m_refcnt.oid);
    }

    /**
     * synthetic fs populate/enumerate/mutate test
     */
    public static void
    fstestRecover(
            AbstractRuntime tTR,
            StreamFactory tsf,
            String strBTreeClass,
            String strInitPath,
            String strWkldPath,
            String strCrashLogPath,
            int nRecoverOp
        ) {
        Pair<Long, Long> oids = recoverOIDs(strCrashLogPath);
        long btreeOID = oids.first;
        long indexOID = oids.second;
        System.out.format("Recovering BTreeFS btreeOID:%d indexOID %d\n", btreeOID, indexOID);
        BTreeFS fs = BTreeFS.attachFS(tTR, tsf, strBTreeClass, btreeOID, indexOID);
        FileSystemDriver driver = new FileSystemDriver(fs, strInitPath, strWkldPath);
        System.out.format("test case:\n%s\n", driver.toString());
        driver.playFrom(nRecoverOp);
        System.out.println("FS after test:\n" + fs.printFS());
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

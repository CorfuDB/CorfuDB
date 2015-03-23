package org.corfudb.tests;
import org.corfudb.runtime.AbstractRuntime;
import org.corfudb.runtime.DirectoryService;
import org.corfudb.runtime.StreamFactory;
import org.corfudb.runtime.collections.CDBAbstractBTree;
import org.corfudb.runtime.collections.CDBLogicalBTree;
import org.corfudb.runtime.collections.CDBPhysicalBTree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
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

    protected CDBAbstractBTree<String, FSEntry> m_btree;
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


    public static class FSEntry implements Serializable {

        public FSEntry parent;
        public String name;
        public FSAttrs attrs;
        public etype type;
        public int numChildren;
        public String[] children;

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
            children = null;
            numChildren = 0;
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
            children = null;
            numChildren = 0;
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
         * toString
         * @return
         */
        public String toString() {
            switch(type) {
                case dir:
                    return "DIR: " + name + "/[c-cnt:"+ children.length + "]";
                case file:
                    return "FILE: " + name + "[" + attrs.size + "KB]";
            }
            return "";
        }

        /**
         * add a child entry to dirnode
         * @param child
         */
        public void addChild(FSEntry child) {
            if(type != etype.dir)
                throw new RuntimeException("Invalid operation!");
            String[] ochildren = children;
            numChildren++;
            children = new String[numChildren];
            for(int i=0; i<numChildren-1; i++)
                children[i] = ochildren[i];
            children[numChildren-1] = child.name;
        }

        /**
         * remove a child
         * @param child
         * @return
         */
        public boolean removeChild(String child) {
            if(type != etype.dir)
                throw new RuntimeException("Invalid operation!");
            if(numChildren <= 0)
                throw new RuntimeException("Invalid operation on empty directory!!");
            ArrayList<String> survivors = new ArrayList<String>();
            for(String s : children) {
                if(s.compareTo(child) != 0)
                    survivors.add(s);
            }
            if(survivors.size() < numChildren) {
                int i = 0;
                children = new String[survivors.size()];
                for (String s : survivors) {
                    children[i++] = s;
                }
                numChildren = survivors.size();
                return true;
            }
            return false;
        }


    }

    /**
     * create a new BTree
     * @param tTR
     * @param tsf
     * @param strBTreeClass
     * @return
     */
    protected CDBAbstractBTree<String, FSEntry>
    createBTree(
        AbstractRuntime tTR,
        StreamFactory tsf,
        String strBTreeClass
        ) {
        long toid = DirectoryService.getUniqueID(tsf);
        if (strBTreeClass.compareTo("CDBPhysicalBTree") == 0) {
            return new CDBPhysicalBTree<String, FSEntry>(tTR, tsf, toid);
        } else if (strBTreeClass.compareTo("CDBLogicalBTree") == 0) {
            return new CDBLogicalBTree<String, FSEntry>(tTR, tsf, toid);
        } else {
            throw new RuntimeException("btree class unknown--cannot create BTreeFS!");
        }
    }

    /**
     * ctor
     * @param nMinAtom
     * @param nMaxAtom
     * @param nMaxDirEntries
     * @param nMaxHeight
     */
    public
    BTreeFS(
            AbstractRuntime tTR,
            StreamFactory tsf,
            int nMinAtom,
            int nMaxAtom,
            int nMaxDirEntries,
            int nMaxHeight,
            String strBTreeClass
        ) {
        m_btree = createBTree(tTR, tsf, strBTreeClass);
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
                strBTreeClass);
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
            int height
    ) {
        BTreeFS fs = new BTreeFS(tTR, tsf, strBTreeClass);
        Random rnd = new Random();
        FSEntry root = new FSEntry("root", null, etype.dir, Long.MIN_VALUE, 0);
        fs.populateRandomFS(rnd, root, minIdLength, maxIdLength, maxChildren, dirProbability, height);
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
            int height
        )
    {
        int nChildren = rnd.nextInt(maxChildren);
        parent.children = new String[nChildren];
        put(parent.path(), parent);
        for(int i=0; i<nChildren; i++) {
            double nextDirProbability = height == 0 ? 0.0 : dirProbability;
            FSEntry child = randomChild(rnd, parent, minIdLength, maxIdLength, nextDirProbability);
            if(child.type == etype.dir) {
                assert(height > 0);
                populateRandomFS(rnd, child, minIdLength, maxIdLength, maxChildren, dirProbability, height-1);
            } else {
                put(child.path(), child);
            }
            parent.children[i] = child.name;
            parent.numChildren = i+1;
            update(parent.path(), parent);      // commit
        }
    }

    /**
     * rename
     * @param tTR
     * @param path
     * @return
     */
    public boolean
    rename(
            AbstractRuntime tTR,
            String path,
            String newName
        ) {
        throw new RuntimeException("unimplemented!");
    }


    /**
     * open file
     * @param tTR
     * @param path
     * @return
     */
    public FSEntry
    openFile(
            AbstractRuntime tTR,
            String path
        ) {
        boolean inTX = false;
        boolean done = false;
        FSEntry file = null;
        while(!done) {
            try {
                if (tTR != null) {
                    tTR.BeginTX();
                    inTX = true;
                }
                file = get(path);
                done = tTR == null ? true : tTR.EndTX();
                inTX = false;
            } catch (Exception e) {
                if (inTX) tTR.AbortTX();
                inTX = false;
                if (tTR != null)
                    throw e;
            }
        }
        return file;
    }

    /**
     * open file
     * @param tTR
     * @param name
     * @return
     */
    public List<FSEntry>
    search(
            AbstractRuntime tTR,
            String name
        ) {
        ArrayList<FSEntry> matches = null;
        boolean inTX = false;
        boolean done = false;
        FSEntry file = null;
        while(!done) {
            try {
                if (tTR != null) {
                    tTR.BeginTX();
                    inTX = true;
                }
                FSEntry root = get("root");
                matches = search(root, name);
                done = tTR == null ? true : tTR.EndTX();
                inTX = false;
            } catch (Exception e) {
                if (inTX) tTR.AbortTX();
                inTX = false;
                if (tTR != null)
                    throw e;
            }
        }
        return matches;
    }

    /**
     * search for matching files
     * @return
     */
    protected ArrayList<FSEntry> search(FSEntry fs, String fname) {
        ArrayList<FSEntry> matches = new ArrayList<FSEntry>();
        if(fs.type == etype.file) {
            if(fs.name.compareTo(fname) == 0)
                matches.add(fs);
        } else {
            for(int i=0; i<fs.numChildren; i++) {
                String path = fs.path() + "/" + fs.children[i];
                FSEntry child = get(path);
                if(child.name.compareTo(fname) == 0)
                    matches.add(child);
                matches.addAll(search(child, fname));
            }
        }
        return matches;
    }

    /**
     * delete a file
     * @param tTR
     * @param path
     * @return
     */
    public boolean
    deleteFile(
            AbstractRuntime tTR,
            String path
    ) {
        boolean inTX = false;
        boolean done = false;
        boolean result = false;
        while(!done) {
            try {
                if (tTR != null) {
                    tTR.BeginTX();
                    inTX = true;
                }
                FSEntry file = get(path);
                if(file != null && file.type == etype.file) {
                    FSEntry parent = get(file.parent.path());
                    result = remove(path) != null;
                    result &= parent.removeChild(file.name);
                }
                done = tTR == null ? true : tTR.EndTX();
                inTX = false;
            } catch (Exception e) {
                if (inTX) tTR.AbortTX();
                inTX = false;
                if (tTR != null)
                    throw e;
            }
        }
        return result;
    }

    /**
     * create a new file
     * @param tTR
     * @param fsName
     * @param parentPath
     * @param permissions
     * @return
     */
    public boolean
    createFile(
        AbstractRuntime tTR,
        String fsName,
        String parentPath,
        long permissions
        ) {
        boolean inTX = false;
        boolean done = false;
        while(!done) {
            try {
                if (tTR != null) {
                    tTR.BeginTX();
                    inTX = true;
                }
                FSEntry parent = get(parentPath == null ? "root" : parentPath);
                FSEntry file = new FSEntry(fsName, parent, etype.file, 0, permissions);
                parent.addChild(file);
                put(file.path(), file);
                update(parent.path(), parent);
                done = tTR == null ? true : tTR.EndTX();
                inTX = false;
            } catch (Exception e) {
                if (inTX) tTR.AbortTX();
                inTX = false;
                if (tTR != null)
                    throw e;
            }
        }
        return true;
    }

    /**
     * create a new directory
     * @param tTR
     * @param fsName
     * @param parentPath
     * @param permissions
     * @return
     */
    public boolean
    createDirectory(
            AbstractRuntime tTR,
            String fsName,
            String parentPath,
            long permissions
        ) {
        boolean inTX = false;
        boolean done = false;
        while(!done) {
            try {
                if (tTR != null) {
                    tTR.BeginTX();
                    inTX = true;
                }
                FSEntry parent = get(parentPath == null ? "root" : parentPath);
                FSEntry dir = new FSEntry(fsName, parent, etype.dir, 0, permissions);
                parent.addChild(dir);
                put(dir.path(), dir);
                update(parent.path(), parent);
                done = tTR == null ? true : tTR.EndTX();
                inTX = false;
            } catch (Exception e) {
                if (inTX) tTR.AbortTX();
                inTX = false;
                if (tTR != null)
                    throw e;
            }
        }
        return true;
    }

    /**
     * print the b-tree (not the file system tree)
     * @return
     */
    public String printBTree() { return m_fs.print(); }

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
        StringBuilder sb = new StringBuilder();
        sb.append(indent);
        if(fs.type == etype.file) {
            sb.append("*");
            sb.append(fs.name);
            sb.append("\n");
        } else {
            sb.append(fs.name);
            sb.append("/\n");
            for(int i=0; i<fs.numChildren; i++) {
                String path = fs.path() + "/" + fs.children[i];
                FSEntry child = get(path);
                sb.append(printFS(child, indent+"  "));
            }
        }
        return sb.toString();
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
        BTreeFS fs = BTreeFS.createRandomFS(tTR, tsf, strBTreeClass,
                minIdLength, maxIdLength, maxChildren, dirProbability, height);
        System.out.println("FS-tree:\n"+fs.printBTree());
        System.out.println("FS:\n"+fs.printFS());
    }

}

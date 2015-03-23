package org.corfudb.tests;
import org.corfudb.runtime.AbstractRuntime;
import org.corfudb.runtime.DirectoryService;
import org.corfudb.runtime.StreamFactory;
import org.corfudb.runtime.collections.CDBAbstractBTree;
import org.corfudb.runtime.collections.CDBLogicalBTree;
import org.corfudb.runtime.collections.CDBPhysicalBTree;
import org.reflections.vfs.Vfs;

import java.io.Serializable;
import java.util.Date;
import java.util.Random;


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

    protected CDBAbstractBTree<String, FSEntry> m_fs;
    protected int m_nMinAtom;
    protected int m_nMaxAtom;
    protected int m_nMaxDirEntries;
    protected int m_nMaxHeight;

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
        m_fs = createBTree(tTR, tsf, strBTreeClass);
        m_nMinAtom = nMinAtom;
        m_nMaxAtom = nMaxAtom;
        m_nMaxDirEntries = nMaxDirEntries;
        m_nMaxHeight = nMaxHeight;
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
        populateRandomFS(fs.m_fs, rnd, root, minIdLength, maxIdLength, maxChildren, dirProbability, height);
        return fs;
    }

    /**
     * given a parent directory, populate it.
     * @param fs
     * @param rnd
     * @param parent
     * @param minIdLength
     * @param maxIdLength
     * @param maxChildren
     * @param dirProbability
     * @param height
     */
    protected static void
    populateRandomFS(
            CDBAbstractBTree<String, FSEntry> fs,
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
        fs.put(parent.path(), parent);
        for(int i=0; i<nChildren; i++) {
            double nextDirProbability = height == 0 ? 0.0 : dirProbability;
            FSEntry child = randomChild(rnd, parent, minIdLength, maxIdLength, nextDirProbability);
            if(child.type == etype.dir) {
                assert(height > 0);
                populateRandomFS(fs, rnd, child, minIdLength, maxIdLength, maxChildren, dirProbability, height-1);
            } else {
                fs.put(child.path(), child);
            }
            parent.children[i] = child.name;
            parent.numChildren = i+1;
            fs.update(parent.path(), parent);      // commit
        }
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
        FSEntry root = m_fs.get("root");
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
                FSEntry child = m_fs.get(path);
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

/**
 * CorfuDB client binding for YCSB.
 *
 * All YCSB records are mapped to a CorfuDB map entry.  
 * TBD: how to support range queries/scans? 
 */

package org.corfudb.runtime;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import org.corfudb.client.CorfuDBClient;
import org.corfudb.runtime.collections.CorfuDBMap;
import org.corfudb.runtime.collections.CDBOrderedMap;

public class YCSBClientMM extends DB {

    private CorfuDBClient crf;
    private String masternode;
    private String rpchostname;
    private IStreamFactory sf;
    private AbstractRuntime TR = null;
    private DirectoryService DS = null;
    private CorfuLogAddressSpace addressSpace = null;
    private CorfuStreamingSequencer sequencer = null;
    private int rpcport = 9090;
    private CorfuDBMap<String, Map<String, String>> map = null;
    private CDBOrderedMap<Double, String> index = null;

    public static final String DEFAULT_RPCPORT = "9090";
    public static final String DEFAULT_MASTERNODE = "http://localhost:8002/corfu";
    public static final String MASTERNODE_PROPERTY = "corfudb.masternode";
    public static final String RPCPORT_PROPERTY = "corfudb.rpcport";

    public static final String INDEX_KEY = "_indices";

    public void init() throws DBException {

        Properties props = getProperties();
        masternode = props.getProperty(MASTERNODE_PROPERTY, DEFAULT_MASTERNODE);
        rpcport = Integer.parseInt(props.getProperty(RPCPORT_PROPERTY, DEFAULT_RPCPORT));
        crf = new CorfuDBClient(masternode);
        crf.startViewManager();
        crf.waitForViewReady();

        try {
            rpchostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        addressSpace = new CorfuLogAddressSpace(crf, 0);
        sequencer = new CorfuStreamingSequencer(crf);
        sf = new IStreamFactoryImpl(addressSpace, sequencer);
        TR = new TXRuntime(sf, DirectoryService.getUniqueID(sf), rpchostname, rpcport);
        DS = new DirectoryService(TR);
        map = new CorfuDBMap(TR, DS.nameToStreamID("ycsbmap"));
        index = new CDBOrderedMap(TR, DS.nameToStreamID("ycsbindex"));

    }

    public void cleanup() throws DBException {
        // TBD--anything to do here?
    }

    private double hash(String key) {
        return key.hashCode();
    }

    @Override
    public int
    read(String table,
         String key,
         Set<String> fields,
         HashMap<String, ByteIterator> result
        )
    {
        if (fields == null) {
            StringByteIterator.putAllAsByteIterators(result, map.get(key));
        }
        else {
            String[] fieldArray = (String[])fields.toArray(new String[fields.size()]);
            Map<String, String> entry = map.get(key);
            Iterator<String> fieldIterator = fields.iterator();
            while (fieldIterator.hasNext()) {
                String field = fieldIterator.next();
                result.put(field, new StringByteIterator(entry.get(field)));
            }
        }
        return result.isEmpty() ? 1 : 0;
    }

    @Override
    public int
    insert(String table,
           String key,
           HashMap<String, ByteIterator> values
        )
    {
        map.put(key, StringByteIterator.getStringMap(values));
        index.put(hash(key), key);
        return 0;
    }

    @Override
    public int
    delete(String table,
           String key
        )
    {
        map.remove(key);
        index.remove(hash(key));
        return 0;
    }

    @Override
    public int
    update(String table,
           String key,
           HashMap<String, ByteIterator> values
        )
    {
        map.put(key, StringByteIterator.getStringMap(values));
        return 0;
    }

    @Override
    public int
    scan(String table,
         String startkey,
         int recordcount,
         Set<String> fields,
         Vector<HashMap<String, ByteIterator>> result
        )
    {
        SortedMap<Double, String> range = index.getRange(hash(startkey), recordcount);
        Collection<String> keys = range.values();
        HashMap<String, ByteIterator> values;
        for (String key : keys) {
            values = new HashMap();
            read(table, key, fields, values);
            result.add(values);
        }
        return 0;
    }

}

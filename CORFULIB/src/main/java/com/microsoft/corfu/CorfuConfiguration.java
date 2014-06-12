/**
 * 
 */
package com.microsoft.corfu;

import java.io.*;
import java.util.ArrayList;
import javax.security.auth.login.Configuration;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

/**
 *
 */
public class CorfuConfiguration 
{
	Logger log = LoggerFactory.getLogger(CorfuConfiguration.class);

    int globalepoch;
    ArrayList<SegmentView> segmentlist = new ArrayList<SegmentView>();

    /** Constructor from a general input-source
     * @param is
     */
    public CorfuConfiguration(InputSource is) throws CorfuException {
        Document doc = null;

        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setNamespaceAware(true);

            DocumentBuilder db = dbf.newDocumentBuilder();

            doc = db.parse(is);
            DOMToConf(doc);
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new CorfuException("Error parsing XML config file!");
        }
    }

    /**
     * Constructor from string
     * @param inp
     */
    public CorfuConfiguration(InputStream inp) throws CorfuException {
        this(new InputSource(inp));
    }

    /**
     * Constructor from string
     * @param bootstrapconfiguration
     */
    public CorfuConfiguration(String bootstrapconfiguration) throws CorfuException {
        this(new InputSource(new java.io.StringReader(bootstrapconfiguration)));
    }

    /** Constructor from file
     * @param bootstraplocation
     */
    public CorfuConfiguration(File bootstraplocation) throws CorfuException, FileNotFoundException {
        this(new InputSource(new FileReader(bootstraplocation)));
    }

    /**
     * copy constructor
     * @param cloned
     */
    public CorfuConfiguration(CorfuConfiguration cloned) {
        globalepoch = cloned.globalepoch;
        segmentlist = new ArrayList<SegmentView>();
        for (SegmentView s : cloned.segmentlist) {
            segmentlist.add(new SegmentView(s));
        }
    }
    /**
     * @return the global epoch of this configuration
     * Individual segments of the configuration may have lower epochs than the global configuration, 
     * if they never changed. (unused for now) 
     */
    public int getGlobalEpoch() { return globalepoch; }

    public void changeEpoch(int ne) { globalepoch = ne; }

    SegmentView getActiveSegmentView() { return segmentlist.get(segmentlist.size()-1); }

	public int getNumSegments() { return segmentlist.size(); }
	
	/** obtain the count of distinct replica-sets
	 * @return the number of replica-sets in the currently-active segment
	 */
	public int getNumGroups() { return getActiveSegmentView().groups.length; }
	
	/** Obtain an array of replicas
	 * @param ind : is the index of the replica-set within in the currently-active segment
	 * @return the array of Endpoint's of the requested replica-set
	 */
	public Endpoint[] getGroupByNumber(int ind) { return getActiveSegmentView().groups[ind].replicas; }
	
	/** Obtain the size of a replica-set
	 * @param ind : is the index of the replica-set within in the currently-active segment
	 * @return the number of Endpoint's in the requested replica-set
	 */
	public int getGroupsizeByNumber(int ind) { return getActiveSegmentView().groups[ind].numnodes; }
	
	/** Obtain the sequencer of the currently active segment
	 * @return Endpoint of the sequencer
	 */
	public Endpoint getSequencer() { return getActiveSegmentView().tokenserver; }
	
	/** Obtain the grain size of the currently active segment 
	 * (TODO shouldn't this permanent and not change with each segment??)
	 * @return grain-size of current segment
	 */
	public int getGrain() { return getActiveSegmentView().grain; }
	
	/** Obtain the unit size of the currently active segment 
	 * (TODO shouldn't this permanent and not change with each segment??)
	 * @return disk-size of current segment
	 */
	public int getUnitsize() { return getActiveSegmentView().disksize; }
	
	/**
	 * Obtain the capacity (in grain-size unit) of the current active segment
	 * @return capacity (in grain-size units) of the current active segment
	 */
	public int getCapacity() { return getActiveSegmentView().disksize * getActiveSegmentView().numgroups; }


    void DOMToConf(Document doc) throws Exception {

        Node N = doc.getElementsByTagName("systemview").item(0);
        int numsegments = Integer.parseInt(N.getAttributes().getNamedItem("NumConfigs").getNodeValue());

        globalepoch = Integer.parseInt(N.getAttributes().getNamedItem("GlobalEpoch").getNodeValue());

        // log is mapped onto
        // - list of SegmentView's
        //   -- each segment striped over a list of replicas GroupViews
        //      -- each group has a list of ChildNodes
        //

        for (int i = 0; i < numsegments; i++) {
            Node segmentN = doc.getElementsByTagName("CONFIGURATION").item(i);
            int segmentindex = Integer.parseInt(segmentN.getAttributes().getNamedItem("index").getNodeValue());
            int numgroups = Integer.parseInt(segmentN.getAttributes().getNamedItem("numgroups").getNodeValue());
            int startoff = Integer.parseInt(segmentN.getAttributes().getNamedItem("startoffset").getNodeValue());
            int grain = Integer.parseInt(segmentN.getAttributes().getNamedItem("grain").getNodeValue());
            int disksize = Integer.parseInt(segmentN.getAttributes().getNamedItem("disksize").getNodeValue());
            String tokenserveraddress = segmentN.getAttributes().getNamedItem("tokenserver").getNodeValue();
            Endpoint tokenserver = new Endpoint(tokenserveraddress);

            GroupView[] grouplist = new GroupView[numgroups];
            log.info("globalepoch {}Segment {} with {} group(s) [{}..{}] grain={}",
                    globalepoch,
                    segmentindex, numgroups, startoff, startoff + disksize, grain);

            for (int j = 0; j < segmentN.getChildNodes().getLength(); j++) {
                long localstartoff = 0;
                Node groupN = segmentN.getChildNodes().item(j);

                if (!(groupN.getNodeType() == Node.ELEMENT_NODE && groupN.hasAttributes())) continue;
                int gindex = Integer.parseInt(groupN.getAttributes().getNamedItem("index").getNodeValue());
                int groupepoch = Integer.parseInt(groupN.getAttributes().getNamedItem("groupepoch").getNodeValue());
                int numnodes = Integer.parseInt(groupN.getAttributes().getNamedItem("numnodes").getNodeValue());
                int groupID = Integer.parseInt(groupN.getAttributes().getNamedItem("groupID").getNodeValue());

                if (numnodes > 1) {
                    log.error("replication not supported yet");
                    System.exit(0);
                }
                log.info("group {} has {} units", gindex, numnodes);

                Endpoint[] corfunodes = new Endpoint[numnodes];
                for (int k = 0; k < groupN.getChildNodes().getLength(); k++) {
                    Node nodeN = groupN.getChildNodes().item(k);
                    if (!(nodeN.getNodeType() == Node.ELEMENT_NODE && nodeN.hasAttributes())) continue;
                    String nodeaddress = nodeN.getAttributes().getNamedItem("nodeaddress").getNodeValue();
                    int nindex = Integer.parseInt(nodeN.getAttributes().getNamedItem("index").getNodeValue());
                    log.info("node[{}]: {}", nindex, nodeaddress);

                    int curlocalstartoff = Integer.parseInt(nodeN.getAttributes().getNamedItem("startoffset").getNodeValue());
                    if (j != 0 && curlocalstartoff != localstartoff)
                        throw new Exception("Bad config format: replicas must have same startoff");
                    else localstartoff = curlocalstartoff;
                    corfunodes[nindex] = new Endpoint(nodeaddress);
                }
                grouplist[gindex] = new GroupView(localstartoff, corfunodes, groupepoch, numnodes, groupID);
            }

            segmentlist.add(new SegmentView(startoff, numgroups, disksize, grain, grouplist, tokenserver));

        }
    }

    /**
     * export the current configuration in XML format into a DOM
     * @return a Document representation of the history of configuration-segments
     *
     * The XML result has the following template structure:
     * <systemview LogID="0" SequenceNo="0" GlobalEpoch="0" NumConfigs="1">
    <CONFIGURATION index="0" startoffset="0" tokenserver="localhost:9020" disksize="1000" grain="128" numgroups="2">
    <GROUP index="0" groupID="1" groupepoch="0" numnodes="1">
    <NODE index="0" nodeaddress="localhost:9040" startoffset="0" />
    </GROUP>
    <GROUP index="1" groupID="2" groupepoch="0" numnodes="1">
    <NODE index="0" nodeaddress="localhost:9045" startoffset="0" />
    </GROUP>
    </CONFIGURATION>
    </systemview>
     */
    public Document ConfToDOM() throws ParserConfigurationException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);

        Document doc = null;
        doc = dbf.newDocumentBuilder().newDocument();
        Element rootElement = doc.createElement("systemview");
        rootElement.setAttribute("GlobalEpoch", Integer.toString(globalepoch));
        rootElement.setAttribute("NumConfigs", Integer.toString(getNumSegments()));
        doc.appendChild(rootElement);

        int sind = 0;
        for (SegmentView s: segmentlist) {
            Element conf = doc.createElement("CONFIGURATION");
            rootElement.appendChild(conf);

            conf.setAttribute("index", Integer.toString(sind));
            sind++;
            conf.setAttribute("startoffset", Long.toString(s.startoff));
            conf.setAttribute("tokenserver", s.tokenserver.toString());
            conf.setAttribute("disksize", Integer.toString(s.disksize));
            conf.setAttribute("grain", Integer.toString(s.grain));
            conf.setAttribute("numgroups", Integer.toString(s.numgroups));

            int gind = 0;
            for (GroupView gv : s.groups) {
                Element grp = doc.createElement("GROUP");
                conf.appendChild(grp);
                grp.setAttribute("index", Integer.toString(gind));
                grp.setAttribute("groupID", Integer.toString(gv.groupid));
                grp.setAttribute("groupepoch", Integer.toString(gv.localepoch));
                grp.setAttribute("numnodes", Integer.toString(gv.numnodes));
                gind++;

                int nind = 0;
                for (Endpoint nd : gv.replicas) {
                    Element node = doc.createElement("NODE");
                    grp.appendChild(node);
                    node.setAttribute("index", Integer.toString(nind));
                    node.setAttribute("nodeaddress", nd.toString());
                    node.setAttribute("startoffset", Long.toString(gv.localstartoff)); // TODO do we need a per node startoffset?
                    nind++;
                }
            }
        }
        return doc;
    }

    /**
     * export the current configuration in XML format into a String
     * @return An String representation in XML format of the series of segements
     * @throws TransformerException
     * @throws ParserConfigurationException
     */
    public String ConfToXMLString() throws CorfuException {
        String s = null;
        try {
            Document doc = ConfToDOM();
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            DOMSource src = new DOMSource(doc);
            StringWriter writer = new StringWriter();
            transformer.transform(src, new StreamResult(writer));
            s = writer.getBuffer().toString();
        } catch (ParserConfigurationException e) {
            throw new InternalCorfuException("failed to convert configuration to DOM");
        } catch (TransformerConfigurationException e) {
            throw new InternalCorfuException("failed to convert configuration to DOM");
        } catch (TransformerException e) {
            throw new InternalCorfuException("failed to convert configuration to DOM");
        }
        return s;
    }

    /**
     * Remove a single logging unit from configuration.
     * We create a new segment which succeeds the current sequence segment-history. The new segments starts at 'highOffset+1'
     * and excludes the removed unit.
     * @param highOffset the highest log-offset reached up to now
     * @param hostname removed unit hostname
     * @param port removed unit port #
     * @return a proposed new configuration
     * @throws TransformerException when an internal problem occurs in transforming to XML string
     * @throws ParserConfigurationException when an internal problem occurs when parsing the configuration
     * @throws BadParamCorfuException when the unit to be removed is either not found, or is a single replica
     */
    public CorfuConfiguration getRemoveUnitProposal(long highOffset, String hostname, int port)
            throws CorfuException {
        CorfuConfiguration newC = new CorfuConfiguration(this);
        newC.changeEpoch(globalepoch+1);

        boolean found = false;
        for (SegmentView s : newC.segmentlist) {
            for (GroupView g : s.groups) {
                if (g.numnodes <= 1) {
                    throw new BadParamCorfuException("cannot remove a unit from a single-replica group!");
                }
                int ind = 0;
                for (Endpoint n : g.replicas) {
                    log.debug("node {}: {}", ind, n.toString());
                    if (n.getHostname().equals(hostname) && n.getPort() == port) {
                        log.info("found unit to remove groupID={} node index={}", g.groupid, ind);
                        g.removeUnit(ind);
                        found = true;
                    }
                    ind++;
                }
            }
        }
        if (!found) throw new BadParamCorfuException(" unit to be removed is not found");
        newC.getActiveSegmentView().setEndoff(highOffset);
        log.info("concatenate new segment");
        SegmentView newseg = new SegmentView(newC.getActiveSegmentView());
        newseg.startoff = highOffset+1;
        log.info("new segment starts at offset {}", newseg.startoff);
        newC.segmentlist.add(newseg);
        return newC;
    }

    /**
     * Remove an entire replica-group from configuration.
     * We create a new segment which succeeds the current sequence segment-history. The new segments starts at 'highOffset+1'
     * and excludes the removed group.
     * @param highOffset the highest log-offset reached up to now
     * @param groupind the index of the removed group (between 0..numgroups-1).
     *
     * @return a proposed new configuration
     * @throws TransformerException when an internal problem occurs in transforming to XML string
     * @throws ParserConfigurationException when an internal problem occurs when parsing the configuration
     * @throws BadParamCorfuException when the unit to be removed is either not found, or is a single replica
     */
    public CorfuConfiguration getRemoveGroupProposal(long highOffset, int groupind)
            throws CorfuException {

        CorfuConfiguration newC = new CorfuConfiguration(this);
        newC.getActiveSegmentView().setEndoff(highOffset);
        newC.changeEpoch(globalepoch+1);

        SegmentView s = new SegmentView(newC.getActiveSegmentView());
        if (s.numgroups < groupind) throw new BadParamCorfuException("replica-group to be removed is not found in current segment");
        s.removeGroup(groupind);
        s.startoff = highOffset+1;
        newC.segmentlist.add(s);

        return newC;
    }

    /**
     * Deploy a new replica-group at the end of configuration.
     * We create a new segment which succeeds the current sequence segment-history. The new segments starts at 'highOffset+1'
     * and includes the new group.
     * @param highOffset the highest log-offset reached up to now
     * @param newgroup the list of logging-unit endpoints of the new replica-group.
     *
     * @return a proposed new configuration
     * @throws TransformerException when an internal problem occurs in transforming to XML string
     * @throws ParserConfigurationException when an internal problem occurs when parsing the configuration
     * @throws BadParamCorfuException when the unit to be removed is either not found, or is a single replica
     */
    public CorfuConfiguration getDeployGroupProposal(long highOffset, Endpoint[] newgroup)
        throws CorfuException {
        CorfuConfiguration newC = new CorfuConfiguration(this);
        newC.getActiveSegmentView().setEndoff(highOffset);
        newC.changeEpoch(globalepoch+1);

        SegmentView s = new SegmentView(newC.getActiveSegmentView());
        s.addGroup(newgroup, globalepoch+1);
        s.startoff = highOffset+1;
        newC.segmentlist.add(s);

        return newC;
    }


	SegmentView getSegmentForOffset(long offset)
	{
        for (SegmentView s : segmentlist) {
            if (s.endoff == -1 || offset < s.endoff) {
                if (offset >= s.startoff) return s;
                else return null;
            }
        }
		return null;
	}

    /**
     * This is a core part of configuration management: The mapping of an absolute log-offset to a tuple (logging-unit, physical-offset)
     * @param offset an absolute log-position
     * @return an EntryLocation object (@see EntryLocation). It contains
     *  - a groupView object, which holds the replica-group of logging-units for the relevant offset
     *  - a relative  offset within each logging-unit that stores this log entry
     */
	EntryLocation getLocationForOffset(long offset)
	{
		EntryLocation ret = new EntryLocation();
	    SegmentView sv = this.getSegmentForOffset(offset);
		long reloff = offset - sv.startoff;
		
		//select the group using a simple modulo mapping function
		int gnum = (int)(reloff%sv.numgroups);	
		ret.group = sv.groups[gnum];
		ret.relativeOff = reloff/sv.numgroups + ret.group.localstartoff;

        log.info("location({}): seg.startOff={} gnum={} group-startOff={} relativeOff={} ",
                offset,
                sv.startoff,
                gnum,
                ret.group.localstartoff, ret.relativeOff);

		return ret;
	}
}

/**
 * Holds information on where a single log-entry is stores.
 * @field relativeOff holds  the relative offset of the entry within each one of the units
 */
class EntryLocation
{
    /* @field group describes the replica-set of logging-units which stores the  log entry,*/

    /**
     *
     */
    GroupView group;
	long relativeOff;
};

/**
 * A SegmentView represents the view for a segment of the global address space of the Corfu log. It consists of a list of GroupViews;
 * entries are distributed across the GroupViews round-robin.
 * @author maheshba
 *
 */
class SegmentView
{
	long startoff;
    long endoff;
	int numgroups;
	int disksize;
	int grain;
	GroupView[] groups;
	Endpoint tokenserver;
	public SegmentView(long startoff, int numgroups, int disksize, int grain,
			GroupView[] groups, Endpoint tokenserver) {
		super();
		this.startoff = startoff;
		this.numgroups = numgroups;
		this.disksize = disksize;
		this.grain = grain;
		this.groups = groups;
		this.tokenserver = tokenserver;
        this.endoff = -1;
	}
    public long getEndoff() {
        return endoff;
    }

    public void setEndoff(long endoff) {
        this.endoff = endoff;
    }

    public SegmentView(SegmentView cloned) {
        this(cloned.startoff,
                cloned.numgroups,
                cloned.disksize,
                cloned.grain,
                new GroupView[cloned.numgroups],
                cloned.tokenserver);
        for (int i = 0; i < numgroups; i++)
            groups[i] = new GroupView(cloned.groups[i]);
    }

    void removeGroup(int groupind) {
        GroupView[] tempg = new GroupView[groups.length-1];
        if (groupind > 0)
            System.arraycopy(groups, 0, tempg, 0, groupind);
        if (groupind < groups.length-1)
            System.arraycopy(groups, groupind+1, tempg, groupind, groups.length-1-groupind);
        groups = tempg;
        numgroups--;

    }

    void addGroup(Endpoint[] newgroup, int localepoch) {
        GroupView[] tempg = new GroupView[groups.length+1];
        System.arraycopy(groups, 0, tempg, 0, groups.length);
        tempg[groups.length] = new GroupView(0 /* TODO */,
                newgroup,
                localepoch /* TODO*/,
                newgroup.length,
                groups.length);
        groups = tempg;
    }
}


/**
 * A GroupView represents a replica set of nodes and a local start offset within each replica (which is equal across all the replicas).
 * For example, a GroupView of A,B,C with a localstartoff of 50 means that the first entry in this group resides at A:50, B:50 and C:50.
 * @author maheshba
 *
 */
class GroupView
{	
	long localstartoff;
	Endpoint[] replicas;
	int localepoch;
	int numnodes;
	int groupid;

	public GroupView(long localstartoff2, Endpoint[] replicas, int localepoch,
			int numnodes, int groupid) {
		super();
		this.localstartoff = localstartoff2;
		this.replicas = replicas;
		this.localepoch = localepoch;
		this.numnodes = numnodes;
		this.groupid = groupid;		
	}

    public GroupView(GroupView cloned) {
        this(cloned.localstartoff, new Endpoint[cloned.numnodes], cloned.localepoch, cloned.numnodes, cloned.groupid);
        System.arraycopy(cloned.replicas, 0, replicas, 0, cloned.replicas.length);
    }

    void removeUnit(int ind) {
        Endpoint[] tempr = new Endpoint[replicas.length-1];
        if (ind > 0) System.arraycopy(replicas, 0, tempr, 0, ind);
        if (ind < replicas.length-1) System.arraycopy(replicas, ind+1, tempr, ind, replicas.length-1-ind);
        replicas = tempr;
        numnodes--;
    }
}

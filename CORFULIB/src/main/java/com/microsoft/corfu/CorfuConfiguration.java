/**
 * 
 */
package com.microsoft.corfu;

import java.io.*;
import java.util.ArrayList;
import java.util.Vector;
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
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 *
 */
public class CorfuConfiguration 
{
	Logger log = LoggerFactory.getLogger(CorfuConfiguration.class);

    protected int epoch;
    protected int pagesize;
    protected ArrayList<SegmentView> segmentlist = new ArrayList<SegmentView>();
    protected Endpoint sequencer;
    protected long trimmark;

    public long getTrimmark() {
        return trimmark;
    }

    public void setTrimmark(long trimmark) {
        this.trimmark = trimmark;
    }

    public Endpoint getSequencer() {
        return sequencer;
    }

    public int getPagesize() {
        return pagesize;
    }

    public int getEpoch() {
        return epoch;
    }

    public void setEpoch(int epoch) {
        this.epoch = epoch;
    }

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

    SegmentView getActiveSegmentView() { return segmentlist.get(segmentlist.size()-1); }

	/** obtain the count of distinct replica-sets
	 * @return the number of replica-sets in the currently-active segment
	 */
	public int getNumGroups() { return getActiveSegmentView().getNgroups(); }
	
	/** Obtain an array of replicas
	 * @param ind : is the index of the replica-set within in the currently-active segment
	 * @return the array of Endpoint's of the requested replica-set
	 */
	public Vector<Endpoint> getGroupByNumber(int ind) { return getActiveSegmentView().groups.elementAt(ind); }
	
	/** Obtain the size of a replica-set
	 * @param ind : is the index of the replica-set within in the currently-active segment
	 * @return the number of Endpoint's in the requested replica-set
	 */
	public int getNumReplicas(int ind) { return getActiveSegmentView().getNreplicas(); }

    /**
     * import the current configuration in XML format
     *
     * The XML input has the following template structure:
    <CONFIGURATION corfuID="0" epoch="0" sequencer="localhost:9020" pagesize="128">
    <SEGMENT startoffset="0" sealedoffset="-1" ngroups="2" nreplicas="2">
    <GROUP>
    <NODE nodeaddress="localhost:9040" />
    <NODE nodeaddress="localhost:9042" />
    </GROUP>
    <GROUP>
    <NODE nodeaddress="localhost:9045" />
    <NODE nodeaddress="localhost:9048" />
    </GROUP>
    </SEGMENT>
    </CONFIGURATION>
     */
    void DOMToConf(Document doc) throws Exception {

        log.info("parsing XML configuration");

        Node N = doc.getElementsByTagName("CONFIGURATION").item(0);
        epoch = Integer.parseInt(N.getAttributes().getNamedItem("epoch").getNodeValue());
        pagesize = Integer.parseInt(N.getAttributes().getNamedItem("pagesize").getNodeValue());

        // get sequencer
        String sequenceraddress = N.getAttributes().getNamedItem("sequencer").getNodeValue();
        sequencer = Endpoint.genEndpoint(sequenceraddress);
        log.info("epoch={} pagesize={} sequencer={}", epoch, pagesize, sequenceraddress);

        // get trim-mark
        Node T = N.getAttributes().getNamedItem("trim");
        if (T != null)
            trimmark = Long.parseLong(T.getNodeValue());
        else
            trimmark = 0;

        // log is mapped onto
        // - list of SegmentView's
        //   -- each segment striped over a list of replica-groups
        //      -- each group has a list of nodes
        //
        NodeList snodes = N.getChildNodes();
        for (int sind = 0; sind < snodes.getLength(); sind++) {
            Node seg = snodes.item(sind);
            if (!seg.hasAttributes()) continue;
            int ngroups = Integer.parseInt(seg.getAttributes().getNamedItem("ngroups").getNodeValue());
            int nreplicas = Integer.parseInt(seg.getAttributes().getNamedItem("nreplicas").getNodeValue());
            int startoff = Integer.parseInt(seg.getAttributes().getNamedItem("startoffset").getNodeValue());
            long sealedoff = Long.parseLong(seg.getAttributes().getNamedItem("sealedoffset").getNodeValue());
            log.info("segment [{}..{}]: {} groups {} replicas", startoff, sealedoff, ngroups, nreplicas);

            Vector<Vector<Endpoint>> groups = new Vector<Vector<Endpoint>>();
            NodeList gnodes = seg.getChildNodes();
            for (int gind = 0; gind < gnodes.getLength(); gind++) {
                Node group = gnodes.item(gind);
                if (!group.hasChildNodes()) continue;
                log.info("group nodes:");
                Vector<Endpoint> replicas = new Vector<Endpoint>();
                NodeList rnodes = group.getChildNodes();
                for (int rind = 0; rind < rnodes.getLength(); rind++) {
                    Node nnode = rnodes.item(rind);
                    if (! nnode.hasAttributes()) continue;
                    String nodeaddr = nnode.getAttributes().getNamedItem("nodeaddress").getNodeValue();
                    log.info("   node {} ", nodeaddr);
                    replicas.add(Endpoint.genEndpoint(nodeaddr));
                }
                groups.add(replicas);
            }

            segmentlist.add(new SegmentView(ngroups, nreplicas, startoff, sealedoff, false, groups));
        }
        segmentlist.get(segmentlist.size()-1).setIsactive(true);
    }

    /**
     * export the current configuration in XML format into a string
     * @return a Document representation of the history of configuration-segments
     *
     * The XML result has the following template structure:
    <CONFIGURATION corfuID="0" epoch="0" sequencer="localhost:9020" pagesize="128">
    <SEGMENT startoffset="0" sealedoffset="-1" ngroups="2" nreplicas="2">
    <GROUP>
    <NODE nodeaddress="localhost:9040" />
    <NODE nodeaddress="localhost:9042" />
    </GROUP>
    <GROUP>
    <NODE nodeaddress="localhost:9045" />
    <NODE nodeaddress="localhost:9048" />
    </GROUP>
    </SEGMENT>
    </CONFIGURATION>
     */
    public String ConfToXMLString() throws CorfuException {
        return ConfToXMLString(CONFCHANGE.NORMAL, -1, -1, -1, -1, null, -1);
    }

    enum CONFCHANGE  {
        NORMAL,
        REMOVE,
        DEPLOY
    }

    /**
     * export the configuration into XMK string with a proposed modification for
     * removal or deployment of a logging unit.
     *
     * @param flag tells what type of modification to apply to configuration
     * @param segStartOff the starting offset of relevance to this change
     * @param segSealOff tells if part of the segment is to be sealed for use
     * @param segGind the group-index for applying the modification to
     * @param segRind the replica-index for applying the modification to
     * @param hostname the hostname of a new deployed node (if relevant)
     * @param port the port of the new deployed node (if relevant)
     * @return an XML string representation of the modified configuration proposal
     * @throws CorfuException if any of the modifications fails during parsing/generation
     */
    public String ConfToXMLString(CONFCHANGE flag,
                                  long segStartOff, long segSealOff, int segGind, int segRind,
                                  String hostname, int port)
            throws CorfuException {
        String s = null;
        try {
            Document doc = convertConfToDOM(flag, segStartOff, segSealOff, segGind, segRind, hostname, port);

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

    protected Document convertConfToDOM(CONFCHANGE flag,
                                        long segStartOff, long segSealOff, int segGind, int segRind,
                                        String hostname, int port)
            throws ParserConfigurationException {

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);

        Document doc = null;
        doc = dbf.newDocumentBuilder().newDocument();
        Element rootElement = doc.createElement("CONFIGURATION");
        if (flag.equals(CONFCHANGE.NORMAL))
            rootElement.setAttribute("epoch", Integer.toString(epoch));
        else
            rootElement.setAttribute("epoch", Integer.toString(epoch+1)); // TODO is this the right place for this?
        rootElement.setAttribute("nsegments", Integer.toString(segmentlist.size()));
        rootElement.setAttribute("pagesize", Integer.toString(pagesize));
        rootElement.setAttribute("sequencer", sequencer.toString());
        rootElement.setAttribute("trimmark", Long.toString(trimmark));
        doc.appendChild(rootElement);

        for (SegmentView s: segmentlist) {
            Element seg = doc.createElement("SEGMENT");
            rootElement.appendChild(seg);

            seg.setAttribute("startoffset", Long.toString(s.getStartOff()));
            seg.setAttribute("ngroups", Integer.toString(s.getNgroups()));
            seg.setAttribute("nreplicas", Integer.toString(s.getNreplicas()));

            if (flag.equals(CONFCHANGE.REMOVE) && segStartOff == s.getStartOff())
               SegRemovalToDOM(doc, seg, s, segGind, segRind);
            else if (flag.equals(CONFCHANGE.DEPLOY) && segStartOff == s.getStartOff())
                SegDeployToDOM(doc, seg, s, segSealOff, segGind, segRind, hostname, port);
            else
                SegToDOM(doc, seg, s);
        }

        return doc;
    }

    protected void SegToDOM(Document doc, Element seg, SegmentView s)             {
        seg.setAttribute("sealedoffset", Long.toString(s.getSealedOff()));
        for (int gind = 0; gind < s.getNgroups(); gind++) {
            Vector<Endpoint> gv = s.groups.elementAt(gind);
            Element grp = doc.createElement("GROUP");
            seg.appendChild(grp);
            for (Endpoint nd : gv) {
                Element node = doc.createElement("NODE");
                grp.appendChild(node);
                if (nd == null)
                    node.setAttribute("nodeaddress", "TBD:-1");
                else
                    node.setAttribute("nodeaddress", nd.toString());
            }
        }
    }

    protected void SegRemovalToDOM(Document doc, Element seg, SegmentView s, int segGind, int segRind) {
        seg.setAttribute("sealedoffset", Long.toString(s.getSealedOff()));
        for (int gind = 0; gind < s.getNgroups(); gind++) {
            Vector<Endpoint> gv = s.groups.elementAt(gind);
            Element grp = doc.createElement("GROUP");
            seg.appendChild(grp);
            for (int rind = 0; rind < gv.size(); rind++) {
                Endpoint nd = gv.elementAt(rind);
                Element node = doc.createElement("NODE");
                grp.appendChild(node);
                if ((gind == segGind && segRind == rind) || nd== null)
                    node.setAttribute("nodeaddress", "TBD:-1");
                else
                    node.setAttribute("nodeaddress", nd.toString());
            }
        }

    }

    protected void SegDeployToDOM(Document doc, Element seg, SegmentView s,
                                  long segSealOff, int segGind, int segRind, String hostname, int port) {
        seg.setAttribute("sealedoffset", Long.toString(segSealOff));
        for (int gind = 0; gind < s.getNgroups(); gind++) {
            Vector<Endpoint> gv = s.groups.elementAt(gind);
            Element grp = doc.createElement("GROUP");
            seg.appendChild(grp);
            for (int rind = 0; rind < gv.size(); rind++) {
                Endpoint nd = gv.elementAt(rind);
                Element node = doc.createElement("NODE");
                grp.appendChild(node);
                if (gind == segGind && segRind == rind)
                    node.setAttribute("nodeaddress", hostname + ":" + Integer.toString(port));
                else if (nd == null)
                    node.setAttribute("nodeaddress", "TBD:-1");
                else
                    node.setAttribute("nodeaddress", nd.toString());
            }
        }

    }

	SegmentView getSegmentForOffset(long offset)
	{
        for (SegmentView s : segmentlist) {
            if (s.sealedOff == -1 || offset < s.sealedOff) {
                if (offset >= s.startOff) return s;
                else return null;
            }
        }
		return null;
	}

    /**
     * This is a core part of configuration management: The mapping of an absolute log-offset to a tuple (replica-set, physical-offset)
     * @param offset an absolute log-position
     * @return an EntryLocation object (@see EntryLocation). It contains
     *  - a groupView object, which holds the replica-group of logging-units for the relevant offset
     *  - a relative  offset within each logging-unit that stores this log entry
     *
     *  @throws com.microsoft.corfu.TrimmedCorfuException if 'offset' is out of range for the current segment-list
     */
	EntryLocation getLocationForOffset(long offset) throws CorfuException
	{
		EntryLocation ret = new EntryLocation();
	    SegmentView sv = this.getSegmentForOffset(offset);
        if (sv == null) throw new TrimmedCorfuException("cannot get location for offset " + offset);

		long reloff = offset - sv.getStartOff();
		
		//select the group using a simple modulo mapping function
		int gnum = (int)(reloff%sv.getNgroups());
		ret.group = sv.groups.elementAt(gnum);
		ret.relativeOff = reloff/sv.getNgroups();

        log.info("location({}): seg=({}..{}) gnum={} relativeOff={} ",
                offset,
                sv.getStartOff(), sv.getSealedOff(),
                gnum, ret.relativeOff);

		return ret;
	}
}

/**
 * Holds information on where a single log-entry is stores.
 * @field relativeOff holds  the relative offset of the entry within each one of the units
 * @field group describes the replica-set of logging-units which stores the  log entry
 */
class EntryLocation
{
    Vector<Endpoint> group;
	long relativeOff;
};

/**
 * A SegmentView represents the view for a segment of the global address space of the Corfu log. It consists of a list of replica-groups
 * the logg is distributed across the groups in a round-robin manner.
 */
class SegmentView
{
    protected int ngroups, nreplicas; // the pair (ngroups, nreplicas) determine how the log is striped and replicated
    protected long startOff;  // the starting log-offset for which this segment is responsible
    protected long sealedOff; // marks the point of this segment that has already been sealed for appending
    protected boolean isactive; // flag, indicating whether appending is allowed in this segment
    protected Vector<Vector<Endpoint>> groups;

    SegmentView(int ngroups,
                int nreplicas,
                long startOff,
                long sealedOff,
                boolean isactive,
                Vector<Vector<Endpoint>> groups) {
        this.ngroups = ngroups;
        this.nreplicas = nreplicas;
        this.startOff = startOff;
        this.sealedOff = sealedOff;
        this.isactive = isactive;
        this.groups = groups;
    }

    public Vector<Vector<Endpoint>> getGroups() {
        return groups;
    }

    public void setGroups(Vector<Vector<Endpoint>> groups) {
        this.groups = groups;
    }

    public int getNgroups() {
        return ngroups;
    }

    public void setNgroups(int ngroups) {
        this.ngroups = ngroups;
    }

    public int getNreplicas() {
        return nreplicas;
    }

    public void setNreplicas(int nreplicas) {
        this.nreplicas = nreplicas;
    }

    public long getStartOff() {
        return startOff;
    }

    public void setStartOff(long startOff) {
        this.startOff = startOff;
    }

    public long getSealedOff() {
        return sealedOff;
    }

    public void setSealedOff(long sealedOff) {
        this.sealedOff = sealedOff;
    }

    public boolean isIsactive() {
        return isactive;
    }

    public void setIsactive(boolean isactive) {
        this.isactive = isactive;
    }
}

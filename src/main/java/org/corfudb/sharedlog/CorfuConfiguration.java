/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package org.corfudb.sharedlog;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import java.util.Map;
/**
 *
 */
public class CorfuConfiguration {
    Logger log = LoggerFactory.getLogger(CorfuConfiguration.class);

    protected List<Integer> incarnation = new ArrayList<Integer>();

    protected int pagesize;
    protected ArrayList<SegmentView> segmentlist = new ArrayList<SegmentView>();
    protected Endpoint sequencer;
    protected long trimmark;

    public List<Integer> getIncarnation() { return incarnation; }
    public void setIncarnation(List<Integer> incarnation) {
        this.incarnation = incarnation;
    }
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

    //build from map
    public CorfuConfiguration(Map<String,Object> config)
    {
        int epoch = (Integer) config.get("epoch");
        int masterIncarnation = 0;
        int masterId = 0;
        Util.setIncarnation(incarnation, masterIncarnation, masterId, epoch);
        pagesize = (Integer) config.get("pagesize");
        Map<String,Object> sequencerAddr = (Map<String,Object>)config.get("sequencer");
        String sequenceraddress = sequencerAddr.get("address") + ":" + sequencerAddr.get("port");
        sequencer = Endpoint.genEndpoint(sequenceraddress);

        log.info("@C@ incarnation={} pagesize={} trimmark={} sequencer={}", incarnation, pagesize, trimmark, sequenceraddress);


    }

    /**
     * Constructor from a general input-source
     *
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
        } catch (Exception e) {
            e.printStackTrace();
            throw new CorfuException("Error parsing XML config file!");
        }
    }

    /**
     * Constructor from string
     *
     * @param inp
     */
    public CorfuConfiguration(InputStream inp) throws CorfuException {
        this(new InputSource(inp));
    }

    /**
     * Constructor from string
     *
     * @param bootstrapconfiguration
     */
    public CorfuConfiguration(String bootstrapconfiguration) throws CorfuException {
        this(new InputSource(new java.io.StringReader(bootstrapconfiguration)));
    }

    /**
     * Constructor from file
     *
     * @param bootstraplocation
     */
    public CorfuConfiguration(File bootstraplocation) throws CorfuException, FileNotFoundException {

        this(new InputSource(new FileReader(bootstraplocation)));
    }

    public void incIncarnation(int masterid) {
        Util.incMasterEpoch(incarnation, masterid);
    }

    public SegmentView getActiveSegmentView() {
        return segmentlist.get(segmentlist.size() - 1);
    }

    /**
     * obtain the count of distinct replica-sets
     *
     * @return the number of replica-sets in the currently-active segment
     */
    public int getNumGroups() {
        return getActiveSegmentView().getNgroups();
    }

    /**
     * Obtain an array of replicas
     *
     * @param ind : is the index of the replica-set within in the currently-active segment
     * @return the array of Endpoint's of the requested replica-set
     */
    public Vector<Endpoint> getGroupByNumber(int ind) {
        return getActiveSegmentView().groups.elementAt(ind);
    }

    /**
     * Obtain the size of a replica-set
     *
     * @return the number of Endpoint's in the requested replica-set
     */
    public int getNumReplicas() {
        return getActiveSegmentView().getNreplicas();
    }

    public Endpoint getEP(int groupind, int replicaind) {
        return getActiveSegmentView().getGroups().elementAt(groupind).elementAt(replicaind);
    }

    /**
     * import the current configuration in XML format
     * <p/>
     * The XML input has the following template structure:
     * <CONFIGURATION corfuID="0" masterepoch="0" epoch="0" sequencer="localhost:9020" pagesize="128">
     * <SEGMENT startoffset="0" sealedoffset="-1" ngroups="2" nreplicas="2">
     * <GROUP>
     * <NODE nodeaddress="localhost:9040" />
     * <NODE nodeaddress="localhost:9042" />
     * </GROUP>
     * <GROUP>
     * <NODE nodeaddress="localhost:9045" />
     * <NODE nodeaddress="localhost:9048" />
     * </GROUP>
     * </SEGMENT>
     * </CONFIGURATION>
     */
    void DOMToConf(Document doc) throws Exception {

        log.debug("@C@ parsing XML configuration");

        Node N = doc.getElementsByTagName("CONFIGURATION").item(0);

        int epoch = Integer.parseInt(N.getAttributes().getNamedItem("epoch").getNodeValue());
        Node T = N.getAttributes().getNamedItem("masterepoch");
        int masterIncarnation = 0;
        if (T != null)
            masterIncarnation = Integer.parseInt(T.getNodeValue());
        T = N.getAttributes().getNamedItem("masterid");
        int masterId = 0;
        if (T != null)
            masterId = Integer.parseInt(T.getNodeValue());
        Util.setIncarnation(incarnation, masterIncarnation, masterId, epoch);

        pagesize = Integer.parseInt(N.getAttributes().getNamedItem("pagesize").getNodeValue());

        // get sequencer
        String sequenceraddress = N.getAttributes().getNamedItem("sequencer").getNodeValue();
        sequencer = Endpoint.genEndpoint(sequenceraddress);

        // get trim-mark
        T = N.getAttributes().getNamedItem("trimmark");
        if (T != null)
            trimmark = Long.parseLong(T.getNodeValue());
        else
            trimmark = 0;

        log.info("@C@ incarnation={} pagesize={} trimmark={} sequencer={}", incarnation, pagesize, trimmark, sequenceraddress);

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
            log.info("@C@ segment [{}..{}]: {} groups {} replicas", startoff, sealedoff, ngroups, nreplicas);

            Vector<Vector<Endpoint>> groups = new Vector<Vector<Endpoint>>();
            NodeList gnodes = seg.getChildNodes();
            for (int gind = 0; gind < gnodes.getLength(); gind++) {
                Node group = gnodes.item(gind);
                if (!group.hasChildNodes()) continue;
                log.info("@C@ group nodes:");
                Vector<Endpoint> replicas = new Vector<Endpoint>();
                NodeList rnodes = group.getChildNodes();
                for (int rind = 0; rind < rnodes.getLength(); rind++) {
                    Node nnode = rnodes.item(rind);
                    if (!nnode.hasAttributes()) continue;
                    String nodeaddr = nnode.getAttributes().getNamedItem("nodeaddress").getNodeValue();
                    log.info("@C@    node {} ", nodeaddr);
                    replicas.add(Endpoint.genEndpoint(nodeaddr));
                }
                groups.add(replicas);
            }

            segmentlist.add(new SegmentView(ngroups, nreplicas, startoff, sealedoff, false, groups));
        }
    }

    /**
     * export the current configuration in XML format into a string
     *
     * @return a Document representation of the history of configuration-segments
     * <p/>
     * The XML result has the following template structure:
     * <CONFIGURATION corfuID="0" masterepoch="0" epoch="0" sequencer="localhost:9020" pagesize="128">
     * <SEGMENT startoffset="0" sealedoffset="-1" ngroups="2" nreplicas="2">
     * <GROUP>
     * <NODE nodeaddress="localhost:9040" />
     * <NODE nodeaddress="localhost:9042" />
     * </GROUP>
     * <GROUP>
     * <NODE nodeaddress="localhost:9045" />
     * <NODE nodeaddress="localhost:9048" />
     * </GROUP>
     * </SEGMENT>
     * </CONFIGURATION>
     */
    class ConfToXMLConverter {
        Document doc = null;
        Element rootElement = null;

        void init() throws CorfuException {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setNamespaceAware(true);

            try {
                doc = dbf.newDocumentBuilder().newDocument();
            } catch (ParserConfigurationException e) {
                throw new InternalCorfuException("cannot initialize configurationXMLConverter");
            }
        }

        void addGlobalParams(List<Integer> epoch, int nsegments) {
            addGlobalParams(epoch, nsegments, trimmark);
        }

        void addGlobalParams(List<Integer> epoch, int nsegments, long newtrim) {
            rootElement = doc.createElement("CONFIGURATION");
            rootElement.setAttribute("masterepoch", Integer.toString(Util.getMasterEpoch(epoch)));
            rootElement.setAttribute("masterid", Integer.toString(Util.getMasterId(epoch)));
            rootElement.setAttribute("epoch", Integer.toString(Util.getEpoch(epoch)));
            rootElement.setAttribute("nsegments", Integer.toString(nsegments));
            rootElement.setAttribute("pagesize", Integer.toString(pagesize));
            rootElement.setAttribute("sequencer", sequencer.toString());
            rootElement.setAttribute("trimmark", Long.toString(newtrim));
            doc.appendChild(rootElement);
        }

        protected void addSegment(SegmentView s,
                                  long startOffset, long sealedOffset,
                                  int g, int r, String hostname, int port) {
            Element seg = doc.createElement("SEGMENT");
            rootElement.appendChild(seg);

            seg.setAttribute("startoffset", Long.toString(startOffset));
            seg.setAttribute("ngroups", Integer.toString(s.getNgroups()));
            seg.setAttribute("nreplicas", Integer.toString(s.getNreplicas()));
            seg.setAttribute("sealedoffset", Long.toString(sealedOffset));
            for (int gind = 0; gind < s.getNgroups(); gind++) {
                Vector<Endpoint> gv = s.groups.elementAt(gind);
                Element grp = doc.createElement("GROUP");
                seg.appendChild(grp);
                int rind = 0;
                for (Endpoint nd : gv) {
                    Element node = doc.createElement("NODE");
                    grp.appendChild(node);
                    if (gind == g && rind == r)
                        node.setAttribute("nodeaddress", hostname+":"+Integer.toString(port));
                    else if (nd == null)
                        node.setAttribute("nodeaddress", "TBD:-1");
                    else
                        node.setAttribute("nodeaddress", nd.toString());
                    rind++;
                }
            }

        }

        void addNormalSegment(SegmentView s) {
            addSegment(s, s.getStartOff(), s.getSealedOff(), -1, -1, null, -1);
        }

        void addSealedSegment(SegmentView s, long offset) {
            addSegment(s, s.getStartOff(), offset, -1, -1, null, -1);
        }

        void addSegmentRemoval(SegmentView s, int gind, int rind) {
            addSegment(s, s.getStartOff(), s.getSealedOff(), gind, rind, "TBD", -1);
        }

        void addSegmentDeploy(SegmentView s, int gind, int rind, String hostname, int port) {
            addSegment(s, s.getStartOff(), s.sealedOff, gind, rind, hostname, port);
        }

        void addNewSegment(SegmentView s, long offset) {
            addSegment(s, offset, -1, -1, -1, null, -1);
        }

        String toXMLString() throws CorfuException {
            String s = null;

            try {
                Transformer transformer = TransformerFactory.newInstance().newTransformer();
                transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                DOMSource src = new DOMSource(doc);
                StringWriter writer = new StringWriter();
                transformer.transform(src, new StreamResult(writer));
                s = writer.getBuffer().toString();
            } catch (TransformerConfigurationException e) {
                throw new InternalCorfuException("failed to convert configuration to DOM");
            } catch (TransformerException e) {
                throw new InternalCorfuException("failed to convert configuration to DOM");
            }
            return s;
        }
    }


    public String ConfToXMLString() throws CorfuException {
        ConfToXMLConverter converter = new ConfToXMLConverter();
        converter.init();
        converter.addGlobalParams(incarnation, segmentlist.size());
        for (SegmentView s : segmentlist) converter.addNormalSegment(s);
        return converter.toXMLString();
    }

    public String ConfTrim(long newtrim) throws CorfuException {
        ConfToXMLConverter converter = new ConfToXMLConverter();
        converter.init();
        Util.incEpoch(incarnation);
        converter.addGlobalParams(incarnation, segmentlist.size(), newtrim);
        for (SegmentView s : segmentlist) converter.addNormalSegment(s);
        return converter.toXMLString();
    }

    public String ConfRemove(SegmentView rs, int gind, int rind)
            throws CorfuException {

        ConfToXMLConverter converter = new ConfToXMLConverter();
        converter.init();
        Util.incEpoch(incarnation);
        converter.addGlobalParams(incarnation, segmentlist.size());
        for (SegmentView s : segmentlist) {
            if (s.equals(rs))
                converter.addSegmentRemoval(s, gind, rind);
            else
                converter.addNormalSegment(s);
        }
        return converter.toXMLString();
    }

    public String ConfDeploy(SegmentView rs, int gind, int rind, String hostname, int port)
            throws CorfuException {

        ConfToXMLConverter converter = new ConfToXMLConverter();
        converter.init();
        Util.incEpoch(incarnation);
        converter.addGlobalParams(incarnation, segmentlist.size());
        for (SegmentView s : segmentlist) {
            if (s.equals(rs))
                converter.addSegmentDeploy(s, gind, rind, hostname, port);
            else
                converter.addNormalSegment(s);
        }
        return converter.toXMLString();
    }

    public String ConfSeal(long offset)
            throws CorfuException {

        ConfToXMLConverter converter = new ConfToXMLConverter();
        converter.init();
        Util.incEpoch(incarnation);
        converter.addGlobalParams(incarnation, segmentlist.size()+1);
        SegmentView as = getActiveSegmentView();
        for (SegmentView s : segmentlist) {
            if (s.equals(as))
                converter.addSealedSegment(s, offset);
            else
                converter.addNormalSegment(s);
        }
        converter.addNewSegment(as, offset);
        return converter.toXMLString();
    }

    SegmentView getSegmentForOffset(long offset) {
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
     *
     * @param offset an absolute log-position
     * @return an EntryLocation object (@see EntryLocation). It contains
     * - a groupView object, which holds the replica-group of logging-units for the relevant offset
     * - a relative  offset within each logging-unit that stores this log entry
     * @throws TrimmedCorfuException if 'offset' is out of range for the current segment-list
     */
    EntryLocation getLocationForOffset(long offset) throws CorfuException {
        EntryLocation ret = new EntryLocation();
        SegmentView sv = this.getSegmentForOffset(offset);
        if (sv == null) throw new TrimmedCorfuException("cannot get location for offset " + offset);

        long reloff = offset - sv.getStartOff();

        //select the group using a simple modulo mapping function
        int gnum = (int) (reloff % sv.getNgroups());
        ret.group = sv.groups.elementAt(gnum);
        ret.relativeOff = reloff / sv.getNgroups();

        log.debug("@C@ location({}): seg=({}..{}) gnum={} relativeOff={} ",
                offset,
                sv.getStartOff(), sv.getSealedOff(),
                gnum, ret.relativeOff);

        return ret;
    }

    /**
     * Holds information on where a single log-entry is stores.
     *
     * @field relativeOff holds  the relative offset of the entry within each one of the units
     * @field group describes the replica-set of logging-units which stores the  log entry
     */
    class EntryLocation {
        Vector<Endpoint> group;
        long relativeOff;
    }

    ;

    /**
     * A SegmentView represents the view for a segment of the global address space of the Corfu log. It consists of a list of replica-groups
     * the logg is distributed across the groups in a round-robin manner.
     */
    public class SegmentView {
        protected int ngroups, nreplicas; // the pair (ngroups, nreplicas) determine how the log is striped and replicated
        protected long startOff;  // the starting log-offset for which this segment is responsible
        protected long sealedOff; // marks the point of this segment that has already been sealed for appending
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
    }

}

/**
 * 
 */
package com.microsoft.corfu;

import java.io.File;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 *
 */
public class CorfuConfigManager 
{
	Logger log = LoggerFactory.getLogger(CorfuConfigManager.class);

	Configuration C;
	
	public Configuration getCurrentConfiguration()	{ return C; }
	
	public int getNumSegments() { return C.configs.length; }
	
	/** obtain the count of distinct replica-sets
	 * @return the number of replica-sets in the currently-active segment
	 */
	public int getNumGroups() { return C.getActiveSegmentView().groups.length; }
	
	/** Obtain an array of replicas
	 * @param ind : is the index of the replica-set within in the currently-active segment
	 * @return the array of CorfuNode's of the requested replica-set
	 */
	public CorfuNode[] getGroupByNumber(int ind) { return C.getActiveSegmentView().groups[ind].replicas; }
	
	/** Obtain the size of a replica-set
	 * @param ind : is the index of the replica-set within in the currently-active segment
	 * @return the number of CorfuNode's in the requested replica-set
	 */
	public int getGroupsizeByNumber(int ind) { return C.getActiveSegmentView().groups[ind].numnodes; }
	
	/** Obtain the sequencer of the currently active segment
	 * @return CorfuNode of the sequencer
	 */
	public CorfuNode getSequencer() { return C.getActiveSegmentView().tokenserver; }
	
	/** Obtain the grain size of the currently active segment 
	 * (TODO shouldn't this permanent and not change with each segment??)
	 * @return grain-size of current segment
	 */
	public int getGrain() { return C.getActiveSegmentView().grain; }
	
	/** Obtain the unit size of the currently active segment 
	 * (TODO shouldn't this permanent and not change with each segment??)
	 * @return disk-size of current segment
	 */
	public int getUnitsize() { return C.getActiveSegmentView().disksize; }
	
	/**
	 * Obtain the capacity (in grain-size unit) of the current active segment
	 * @return capacity (in grain-size units) of the current active segment
	 */
	public int getCapacity() { return C.getActiveSegmentView().disksize * C.getActiveSegmentView().numgroups; }

	/** Constructor
	 * @param bootstraplocation
	 */
	public CorfuConfigManager(File bootstraplocation)
	{
		try
		{
			log.info("Reading File {}... ", bootstraplocation);
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		    dbf.setNamespaceAware(true);
	
		    DocumentBuilder db=null;
			db = dbf.newDocumentBuilder();
			
			Document doc = null;
		    doc = db.parse(bootstraplocation);
			
		    Node N = doc.getElementsByTagName("systemview").item(0);
		    int numsegments = Integer.parseInt(N.getAttributes().getNamedItem("NumConfigs").getNodeValue());
//		    System.out.println("numsegments = " + numsegments);
		    int globalepoch = Integer.parseInt(N.getAttributes().getNamedItem("GlobalEpoch").getNodeValue());
//		    System.out.println("globalepoch = " + globalepoch);
		    
		    SegmentView[] segmentlist = new SegmentView[numsegments];
		    for(int i=0;i<numsegments;i++)
		    {
		    	Node segmentN = doc.getElementsByTagName("CONFIGURATION").item(i);
		    	int segmentindex = Integer.parseInt(segmentN.getAttributes().getNamedItem("index").getNodeValue());
		    	int numgroups = Integer.parseInt(segmentN.getAttributes().getNamedItem("numgroups").getNodeValue());
		    	int startoff = Integer.parseInt(segmentN.getAttributes().getNamedItem("startoffset").getNodeValue());
		    	int grain = Integer.parseInt(segmentN.getAttributes().getNamedItem("grain").getNodeValue());
		    	int disksize = Integer.parseInt(segmentN.getAttributes().getNamedItem("disksize").getNodeValue());
		    	String tokenserveraddress = segmentN.getAttributes().getNamedItem("tokenserver").getNodeValue();
	    		CorfuNode tokenserver = new CorfuNode(tokenserveraddress);
		    	
		    	GroupView[] grouplist = new GroupView[numgroups];
		    	
		    	log.info("Segment {} with {} group(s) [{}..{}] grain={}",
		    			segmentindex, numgroups, startoff, startoff+disksize, grain);
		    	
		    	for(int j=0;j<segmentN.getChildNodes().getLength();j++)
		    	{
			    	long localstartoff = 0;
		    		Node groupN = segmentN.getChildNodes().item(j);

		    		if(!(groupN.getNodeType()==Node.ELEMENT_NODE && groupN.hasAttributes())) continue;
		    		int gindex = Integer.parseInt(groupN.getAttributes().getNamedItem("index").getNodeValue());
		    		int groupepoch = Integer.parseInt(groupN.getAttributes().getNamedItem("groupepoch").getNodeValue());
		    		int numnodes = Integer.parseInt(groupN.getAttributes().getNamedItem("numnodes").getNodeValue());
		    		int groupID = Integer.parseInt(groupN.getAttributes().getNamedItem("groupID").getNodeValue());
		    				    		
		    		log.info("group {} has {} units", j, groupN.getChildNodes().getLength());

		    		CorfuNode[] corfunodes = new CorfuNode[numnodes];
//		    		for(int k=0;k<numnodes;k++)
		    		for(int k=0;k<groupN.getChildNodes().getLength();k++)
		    		{
		    			Node nodeN = groupN.getChildNodes().item(k);
		    			if(!(nodeN.getNodeType()==Node.ELEMENT_NODE && nodeN.hasAttributes())) continue;
		    			String nodeaddress = nodeN.getAttributes().getNamedItem("nodeaddress").getNodeValue();
		    			log.info("node[{}]: {}", k, nodeaddress);
			    		int nindex = Integer.parseInt(nodeN.getAttributes().getNamedItem("index").getNodeValue());
		    			int curlocalstartoff = Integer.parseInt(nodeN.getAttributes().getNamedItem("startoffset").getNodeValue());
		    			if(j!=0 && curlocalstartoff!=localstartoff) throw new Exception("Bad config format: replicas must have same startoff");
		    			else localstartoff = curlocalstartoff;
		    			corfunodes[nindex] = new CorfuNode(nodeaddress);
		    		}
		    		grouplist[gindex] = new GroupView(localstartoff, corfunodes, groupepoch, numnodes, groupID);
		    	}
			    segmentlist[segmentindex] = new SegmentView(startoff, numgroups, disksize, grain, grouplist, tokenserver);

		    }
		    
		    C = new Configuration(globalepoch, segmentlist);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			throw new RuntimeException("Error parsing XML config file!", e);
		}
	    
	    
	}
	
};

class Configuration
{
	int globalepoch;
	SegmentView[] configs;
	public Configuration(int globalepoch, SegmentView[] configs) {
		super();
		this.globalepoch = globalepoch;
		this.configs = configs;
	}
	SegmentView getActiveSegmentView()
	{
		return configs[configs.length-1];
	}
	SegmentView getSegmentForOffset(long offset)
	{
		if(offset>=configs[configs.length-1].startoff) return configs[configs.length-1];
		if(offset<configs[0].startoff) return null;
		for(int i=1;i<configs.length;i++)
		{
			if(offset<configs[i].startoff) return configs[i-1];
		}
		return null;
	}
	EntryLocation getLocationForOffset(long offset)
	{
		EntryLocation ret = new EntryLocation();
		
		SegmentView sv = this.getSegmentForOffset(offset);
		
		long reloff = offset - sv.startoff;
		
		//select the group using a simple modulo mapping function
		int gnum = (int)(reloff%sv.numgroups);	
		
		ret.group = sv.groups[gnum];
	
		ret.physicaloffset = reloff/sv.numgroups + ret.group.localstartoff;

		return ret;
	}

	
	/** This is a utility function, which computes an array of page-ranges 
	 * for the given offset-range. The range must be within the current active range (for now).
	 * 
	 * @param startoff the starting offset
	 * @param endoff the end offset
	 * @return
	 */
	/*RangeLocation[] getLocationForRange(long startoff, long endoff) {
		SegmentView sv = this.getActiveSegmentView();
		RangeLocation[] ret = new RangeLocation[sv.numgroups];
		
		
		
		return ret;
	}*/
	
}

class RangeLocation
{
	GroupView group;
	long physstartoff, physendoff;
};

class EntryLocation
{
	GroupView group;
	long physicaloffset;
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
	int numgroups;
	int disksize;
	int grain;
	GroupView[] groups;
	CorfuNode tokenserver;
	public SegmentView(long startoff, int numgroups, int disksize, int grain,
			GroupView[] groups, CorfuNode tokenserver) {
		super();
		this.startoff = startoff;
		this.numgroups = numgroups;
		this.disksize = disksize;
		this.grain = grain;
		this.groups = groups;
		this.tokenserver = tokenserver;
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
	CorfuNode[] replicas;
	int localepoch;
	int numnodes;
	int groupid;
	public GroupView(long localstartoff2, CorfuNode[] replicas, int localepoch,
			int numnodes, int groupid) {
		super();
		this.localstartoff = localstartoff2;
		this.replicas = replicas;
		this.localepoch = localepoch;
		this.numnodes = numnodes;
		this.groupid = groupid;		
	}
}

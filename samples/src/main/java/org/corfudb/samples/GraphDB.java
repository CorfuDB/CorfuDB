package org.corfudb.samples;

import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.MutatorAccessor;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by shriyav on 5/25/17.
 */

public class GraphDB {
    private SMRMap<String, Node> vertices;

    public GraphDB() {
        CorfuRuntime runtime = new CorfuRuntime("localhost:9000").connect();
        vertices = runtime.getObjectsView()
                .build()
                .setStreamName("A")     // stream name
                .setType(SMRMap.class) // object class backed by this stream
                .open();                // instantiate the object!
    }

    @Override
    public String toString() {
        return "This graph has " + getNumNodes() + " vertices.";
    }

    public Node getNode(String name) { return vertices.get(name); }

    @MutatorAccessor(name = "addNode")
    public void addNode(String name) { vertices.put(name, new Node(name)); }

    public int getNumNodes() {
        return vertices.size();
    }

    //@MutatorAccessor(name = "addEdge")
    public Edge addEdge(Node from, Node to) {
        Edge e = new Edge(from, to);
        from.addEdge(e);
        to.addEdge(e);
        return e;
    }

    public Edge addEdge(String from, String to) {
        Node f = getNode(from);
        Node t = getNode(to);
        return addEdge(f, t);
    }

    /** Returns an iterable of all vertex IDs in the graph. */
    Iterable<String> vertices() {
        return vertices.keySet();
    }

    /** Returns an iterable of names of all vertices adjacent to v. */
    Iterable<String> adjacent(String v) {
        ArrayList<Edge> edgeList = vertices.get(v).getEdges();
        ArrayList<String> returnVal = new ArrayList<>();
        for (Edge e : edgeList) {
            if (v.equals(e.getTo().getName())) {
                returnVal.add(e.getFrom().getName());
            } else {
                returnVal.add(e.getTo().getName());
            }
        }
        return returnVal;
    }

    /**
     *  Remove nodes with no connections from the graph.
     *  While this does not guarantee that any two nodes in the remaining graph are connected,
     *  we can reasonably assume this since typically roads are connected.
     */
    void clean() {
        // Kept running into ConcurrentModificationException
        // Resolved with:
        // http://stackoverflow.com/questions/1884889/iterating-over-and-removing-from-a-map
        for (Iterator<Map.Entry<String, Node>> it = vertices.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Node> entry = it.next();
            if (entry.getValue().getEdges().size() == 0) {
                it.remove();
            }
        }
    }

    void preDFS(Node f) { // not tested! - edit: need a marked[] array
        System.out.println(f.getName());
        for (Edge neighbor: f.getEdges()) {
            if (neighbor.getFrom().equals(f)) {
                preDFS(neighbor.getTo());
            } else {
                preDFS(neighbor.getFrom());
            }
        }
    }

    void postDFS(Node f) { // not tested! - edit: need a marked[] array
        for (Edge neighbor: f.getEdges()) {
            if (neighbor.getFrom().equals(f)) {
                postDFS(neighbor.getTo());
            } else {
                postDFS(neighbor.getFrom());
            }
        }
        System.out.println(f.getName());
    }

    void BFS(Node f) { // not tested!
        System.out.println(f.getName());
        ArrayList<Node> fringe = new ArrayList<>();
        for (Edge neighbor: f.getEdges()) {
            if (neighbor.getFrom().equals(f)) {
                fringe.add(neighbor.getTo());
            } else {
                fringe.add(neighbor.getFrom());
            }
        }
    }
}

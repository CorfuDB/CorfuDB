package org.corfudb.samples;

import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.MutatorAccessor;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * This tutorial demonstrates a simple Corfu application.
 *
 * Created by dalia on 12/30/16.
 */
public class GraphDB2 {
    private SMRMap<String, Node> vertices;

    @CorfuObject
    static class Node {
        private String name;
        private ArrayList<Edge> edges;

        public Node() {
            name = "";
            edges = new ArrayList<>();
        }

        public Node(String n) {
            name = n;
            edges = new ArrayList<>();
        }

        @Accessor
        public String getName() {
            return name;
        }

        @Accessor
        public ArrayList<Edge> getEdges() {
            return edges;
        }

        @MutatorAccessor(name = "setName")
        public void setName(String n) {
            name = n;
        }

        @MutatorAccessor(name = "addEdge")
        public void addEdge(Edge e) {
            edges.add(e);
        }
    }

    @CorfuObject
    static class Edge {
        private String name;
        private Node from;
        private Node to;

        public Edge(Node f, Node t) {
            name = "";
            from = f;
            to = t;
        }

        @Accessor
        public String getName() {
            return name;
        }

        @Accessor
        public Node getFrom() {
            return from;
        }

        @Accessor
        public Node getTo() {
            return to;
        }

        @MutatorAccessor(name = "setName")
        public void setName(String n) {
            name = n;
        }
    }

    public GraphDB2() {
        vertices = new SMRMap<>();
    }

    public Node getNode(String name) { return vertices.get(name); }

    public void addNode(String name) { vertices.put(name, new Node(name)); }

    public int getNumNodes() {
        return vertices.size();
    }

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
    private void clean() {
        // Kept running into ConcurrentModificationException
        // Resolved with:
        // http://stackoverflow.com/questions/1884889/iterating-over-and-removing-from-a-map
        for (Iterator<Map.Entry<String, Node>> it = vertices.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Node> entry = it.next();
            if (entry.getValue().edges.size() == 0) {
                it.remove();
            }
        }
    }

    private static final String USAGE = "Usage: GraphDB2 [-c <conf>]\n"
            + "Options:\n"
            + " -c <conf>     Set the configuration host and port  [default: localhost:9999]\n";

    /**
     * Internally, the corfuRuntime interacts with the CorfuDB service over TCP/IP sockets.
     *
     * @param configurationString specifies the IP:port of the CorfuService
     *                            The configuration string has format "hostname:port", for example, "localhost:9090".
     * @return a CorfuRuntime object, with which Corfu applications perform all Corfu operations
     */
    private static CorfuRuntime getRuntimeAndConnect(String configurationString) {
        CorfuRuntime corfuRuntime = new CorfuRuntime(configurationString).connect();
        return corfuRuntime;
    }

    public static void main(String[] args) {
        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE)
                        .withVersion(GitRepositoryState.getRepositoryState().describe)
                        .parse(args);
        String corfuConfigurationString = (String) opts.get("-c");

        /**
         * First, the application needs to instantiate a CorfuRuntime,
         * which is a Java object that contains all of the Corfu utilities exposed to applications.
         */
        CorfuRuntime runtime = getRuntimeAndConnect(corfuConfigurationString);

        GraphDB2 d = runtime.getObjectsView()
                .build()
                .setStreamName("A")     // stream name
                .setType(GraphDB2.class)  // object class backed by this stream
                .open();                // instantiate the object!

        d.addNode("Alice" + d.getNumNodes());
        d.addNode("Bob" + d.getNumNodes());
        d.addNode("Charlie" + d.getNumNodes());
        d.addNode("Dana" + d.getNumNodes());

        d.addEdge("Alice", "Bob");
        d.addEdge("Alice", "Charlie");
        d.addEdge("Charlie", "Dana");

        for (String friend : d.adjacent("Alice")) {
            System.out.println(friend);
        }
    }
}

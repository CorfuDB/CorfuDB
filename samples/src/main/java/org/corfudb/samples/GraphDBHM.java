package org.corfudb.samples;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by shriyav on 5/25/17.
 */

public class GraphDBHM {
    private HashMap<String, Node> vertices;

    private Tracer t;
    private boolean isTracing;

    public class Tracer {
        String cat;
        String name;
        int pid;
        long tid;
        long ts;
        String ph;
        String[] args;
        File log;

        public Tracer() {
            cat = "";
            name = "";
            pid = -1;
            tid = -1;
            ts = -1;
            ph = "";
            args = null;
            log = new File("performanceLog.json");
            try {
                if (!log.exists()) {
                    log.createNewFile();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void updateArgs(String c, String n, int p1, long t1, long t2, String p2, String[] a) {
            cat = c;
            name = n;
            pid = p1;
            tid = t1;
            ts = t2;
            ph = p2;
            args = a;
            writeToLog();
        }

        public void writeToLog() {
            try {
                FileWriter fw = new FileWriter(log.getAbsoluteFile(), true);
                BufferedWriter bw = new BufferedWriter(fw);

                bw.write("{");
                bw.write("\"cat\": " + "\"" + t.cat + "\",");
                bw.write("\"pid\": " + t.pid + ",");
                bw.write("\"tid\": " + t.tid + ",");
                bw.write("\"ts\": " + t.ts + ",");
                bw.write("\"ph\": " + "\""  + t.ph + "\",");
                bw.write("\"name\": " + "\""  + t.name + "\",");
                bw.write("\"args\": " + t.args + "},\n");

                bw.close();
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println("Done");
        }
    }

    public void startMethodTrace() {
        isTracing = true;
    }

    public void endMethodTrace() {
        isTracing = false;
    }

    public GraphDBHM() {
        vertices = new HashMap<>();
        t = new Tracer();
        isTracing = false;
    }

    @Override
    public String toString() {
        return "This graph has " + getNumNodes() + " vertices.";
    }

    public Node getNode(String name) { return vertices.get(name); }

    public void addNode(String name) { vertices.put(name, new Node(name)); }

    public void removeNode(String name) { vertices.remove(name); }

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
    Iterable<String> adjacent(String v) { // modify with pointers
        if (isTracing) {
            t.updateArgs("GraphDBHMTest", "adjacent", 0, Thread.currentThread().getId(),
                    System.currentTimeMillis(), "B", null);
        }

        // begin method
        ArrayList<Edge> edgeList = vertices.get(v).getEdges();
        ArrayList<String> returnVal = new ArrayList<>();
        for (Edge e : edgeList) {
            if (v.equals(e.getTo().getName())) {
                returnVal.add(e.getFrom().getName());
            } else {
                returnVal.add(e.getTo().getName());
            }
        }
        // end method

        if (isTracing) {
            t.updateArgs("GraphDBHMTest", "adjacent", 0, Thread.currentThread().getId(),
                    System.currentTimeMillis(), "E", null);
        }
        return returnVal;
    }

    /** Clear entire graph: remove all nodes/edges. */
    void clear() {
        vertices.clear();
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

    private ArrayList<Node> dfsHelper(Node n, String dfsType, ArrayList<Node> ordered, ArrayList<Node> seen) {
        if (n != null && !seen.contains(n)) {
            if (dfsType.equals("pre")) {
                ordered.add(n);
            }
            seen.add(n);
            for (Edge neighbor: n.getEdges()) {
                if (neighbor.getFrom().equals(n)) {
                    dfsHelper(neighbor.getTo(), dfsType, ordered, seen);
                } else {
                    dfsHelper(neighbor.getFrom(), dfsType, ordered, seen);
                }
            }
            if (dfsType.equals("post")) {
                ordered.add(n);
            }
        }
        return ordered;
    }

    ArrayList<Node> preDFS(Node f) {
        if (isTracing) {
            t.updateArgs("GraphDBHMTest", "preDFS", 0, Thread.currentThread().getId(),
                    System.currentTimeMillis(), "B", null);
        }

        // begin method
        ArrayList<Node> returnVal = dfsHelper(f, "pre", new ArrayList<Node>(), new ArrayList<Node>());
        // end method

        if (isTracing) {
            t.updateArgs("GraphDBHMTest", "preDFS", 0, Thread.currentThread().getId(),
                    System.currentTimeMillis(), "E", null);
        }
        return returnVal;
    }

    ArrayList<Node> postDFS(Node f) {
        if (isTracing) {
            t.updateArgs("GraphDBHMTest", "postDFS", 0, Thread.currentThread().getId(),
                    System.currentTimeMillis(), "B", null);
        }

        // begin method
        ArrayList<Node> returnVal = dfsHelper(f, "post", new ArrayList<Node>(), new ArrayList<Node>());
        // end method

        if (isTracing) {
            t.updateArgs("GraphDBHMTest", "postDFS", 0, Thread.currentThread().getId(),
                    System.currentTimeMillis(), "E", null);
        }
        return returnVal;
    }

    ArrayList<Node> BFS(Node f) {
        if (isTracing) {
            t.updateArgs("GraphDBHMTest", "BFS", 0, Thread.currentThread().getId(),
                    System.currentTimeMillis(), "B", null);
        }

        // begin method
        ArrayList<Node> ordered = new ArrayList<>();
        ArrayList<Node> fringe = new ArrayList<>();
        fringe.add(f);
        while (fringe.size() != 0) {
            Node curr = fringe.remove(0);
            if (!ordered.contains(curr)) {
                ordered.add(curr);
                for (Edge neighbor : curr.getEdges()) {
                    if (neighbor.getFrom().equals(curr)) {
                        fringe.add(neighbor.getTo());
                    } else {
                        fringe.add(neighbor.getFrom());
                    }
                }
            }
        }
        // end method

        if (isTracing) {
            t.updateArgs("GraphDBHMTest", "BFS", 0, Thread.currentThread().getId(),
                    System.currentTimeMillis(), "E", null);
        }
        return ordered;
    }

    public static void main(String[] args) {
        GraphDBHM d = new GraphDBHM();

        d.addNode("A");
        d.addNode("B");
        d.addNode("C");
        d.addNode("D");
        d.addNode("E");
        d.addNode("F");
        d.addNode("G");
        d.addNode("H");
        d.addNode("I");
        d.addNode("J");

        d.addEdge("A", "B");
        d.addEdge("A", "C");
        d.addEdge("B", "D");
        d.addEdge("B", "E");
        d.addEdge("B", "F");
        d.addEdge("E", "H");
        d.addEdge("E", "I");
        d.addEdge("C", "G");
        d.addEdge("J", "G");

        d.startMethodTrace();
        for (String friend : d.adjacent("B")) {
            System.out.println(friend);
        } // expect: ADEF
        d.endMethodTrace();

        d.startMethodTrace();
        ArrayList<Node> pre = d.preDFS(d.getNode("A"));
        for (Node item : pre) {
            System.out.println(item.getName());
        } // expect: ABDEHIFCGJ
        d.endMethodTrace();

        d.startMethodTrace();
        ArrayList<Node> post = d.postDFS(d.getNode("A"));
        for (Node item : post) {
            System.out.println(item.getName());
        } // expect: DHIEFBJGCA
        d.endMethodTrace();

        d.startMethodTrace();
        ArrayList<Node> bfs = d.BFS(d.getNode("C"));
        for (Node item : bfs) {
            System.out.println(item.getName());
        } // expect: CAGBJDEFHI
        d.endMethodTrace();

        d.clear();
    }
}

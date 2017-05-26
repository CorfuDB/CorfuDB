package org.corfudb.samples;

/**
 * Created by shriyav on 5/25/17.
 */
public class Edge {
    private String name;
    private Node from;
    private Node to;

    public Edge(Node f, Node t) {
        name = "";
        from = f;
        to = t;
    }

    public String getName() {
        return name;
    }

    public Node getFrom() {
        return from;
    }

    public Node getTo() {
        return to;
    }

    public void setName(String n) {
        name = n;
    }
}

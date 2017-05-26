package org.corfudb.samples;

import java.util.ArrayList;

/**
 * Created by shriyav on 5/25/17.
 */

public class Node {
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

    public String getName() {
        return name;
    }

    public ArrayList<Edge> getEdges() {
        return edges;
    }

    public void setName(String n) {
        name = n;
    }

    public void addEdge(Edge e) {
        edges.add(e);
    }
}

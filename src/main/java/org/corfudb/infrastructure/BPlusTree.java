package org.corfudb.infrastructure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.FileChannel;
import java.util.ArrayList;

/**
 * Equal go to the left. Not thread-safe!! But there should only be one thread building the index..
 *
 * Created by taia on 6/24/15.
 */
public class BPlusTree<K extends Comparable, V> {
    private Logger log = LoggerFactory.getLogger(BPlusTree.class);


    class NodeEntry<K> {
         K key;
        Node rightPtr;

        public NodeEntry() {
        }

        public NodeEntry(K key) {
            this.key = key;
        }

        public NodeEntry(K key, Node rightPtr) {
            this.key = key;
            this.rightPtr = rightPtr;
        }

        public void setRightPtr(Node toMe) {
            rightPtr = toMe;
        }

        public void print() {
            log.info("[({}), {}]", key.toString(), rightPtr);
        }
    }

    // Extends so that NodeEntries in the penultimate layer can still point to LeafEntries while type-checking
    class LeafEntry<K, V> extends NodeEntry<K> {
        V value;

        public LeafEntry() {
        }

        public LeafEntry(K key, V value) {
            super(key);
            this.value = value;
        }

        @Override
         public String toString() {
            return "[(" + key.toString() + ", " + value.toString() + "), " + rightPtr + "]";
        }

    }

    class Node<K> {
        Node<K> parent; // Only the root doesn't have a parent
        Node<K> leftMostPtr;
        ArrayList<NodeEntry<K>> elements;
        int fanout;
        boolean leafNode; // optimization

        public Node(int fanout) {
            this.fanout = fanout;
            elements = new ArrayList(fanout);
        }

        public void setLeaf(boolean leaf) {
            leafNode = leaf;
        }

        public void setParent(Node parent) {
            this.parent = parent;
        }

        public void print() {
            if (leftMostPtr != null)
                leftMostPtr.print();
            for (int i = 0; i < elements.size(); i++) {
                elements.get(i).print();
            }
            log.info("\n");
        }
    }

    private int fanout;
    private Node<K> root;

    public BPlusTree(int fanout) {
        this.fanout = fanout;
        root = new Node(fanout);
        root.setLeaf(true);
    }

    public void insert(K key, V value) {
        Node<K> leaf = findLeaf(root, key);
        insertLeaf(leaf, key, value);
    }

    private void insertNode(Node<K> node, NodeEntry<K> entry) {
        boolean done = false;
        for (int i = 0; i < node.elements.size(); i++) {
            if (entry.key.compareTo(node.elements.get(i).key) < 0){
                node.elements.add(i, entry);
                done = true;
                break;
            }
        }
        if (!done) {
            node.elements.add(entry);
        }
        if (node.elements.size() <= fanout) {
            return;
        }

        // No empty space in the node; we have to split.
        Node<K> newRight = new Node<K>(fanout);
        int numLeft = node.elements.size()/2;
        int pivot = (node.elements.size()-1)/2;
        NodeEntry<K> insertMe = node.elements.remove(pivot);

        for (int i = 0; i < numLeft; i++) {
            newRight.elements.add(node.elements.remove(pivot));
            newRight.elements.get(i).rightPtr.setParent(newRight);
        }
        newRight.leftMostPtr = insertMe.rightPtr;
        newRight.leftMostPtr.setParent(newRight);
        insertMe.rightPtr = newRight;

        Node<K> parent = node.parent;
        if (parent == null) {
            // We were inserting into the root.. so we add a new root.
            parent = new Node<K>(fanout);
            parent.elements.add(insertMe);
            parent.leftMostPtr = node;
            parent.elements.get(0).setRightPtr(newRight);
            newRight.setParent(parent);
            root.setParent(parent);
            root = parent;
        } else {
            newRight.setParent(parent);
            insertNode(parent, insertMe);
        }
    }

    private void insertLeaf(Node<K> node, K key, V value) {
        // Insert the key into this leaf.
        boolean done = false;
        for (int i = 0; i < node.elements.size(); i++) {
            if (key.compareTo(node.elements.get(i).key) < 0){
                node.elements.add(i, new LeafEntry(key, value));
                done = true;
                break;
            }
            if (key.compareTo(node.elements.get(i).key) == 0){
                // the key is already here TODO: return something different? Raise exception?
                return;
            }
        }
        if (!done)
            node.elements.add(new LeafEntry(key, value));
        if (node.elements.size() <= fanout)
            return;

        // No empty space in the leaf; we have to split.
        Node<K> newRight = new Node<K>(fanout);
        int numLeft = (node.elements.size()+1)/2; // So that equal elements will go to left of pivot
        int numRight = node.elements.size() - numLeft;
        NodeEntry<K> newPivot = node.elements.get((node.elements.size()-1)/2);

        for (int i = 0; i < numRight; i++) {
            newRight.elements.add(node.elements.remove(numLeft));
        }
        newRight.setLeaf(true);

        Node<K> parent = node.parent;
        if (parent == null) {
            // We were inserting into the root.. so we add a new root.
            parent = new Node<K>(fanout);
            parent.elements.add(new NodeEntry(newPivot.key));
            parent.leftMostPtr = node;
            newRight.setParent(parent);
            parent.elements.get(0).setRightPtr(newRight);
            root.setParent(parent);
            root = parent;
        } else {
            newRight.setParent(parent);
            insertNode(parent, new NodeEntry(newPivot.key, newRight));
        }

    }

    private V findEntryInLeaf(Node<K> node, K key) {
        assert(node.leafNode);
        for (int i = 0; i < node.elements.size(); i++) {
            if (key.compareTo(node.elements.get(i).key) == 0)
                return ((LeafEntry<K,V>)node.elements.get(i)).value;
        }
        return null;
    }

    private Node<K> findLeaf(Node<K> node, K key) {
        if (node.leafNode) {
            return node;
        }
        if (node.elements.size() == 0)
            throw new RuntimeException("Empty node?!");

        // Check for use of leftPtr
        if (node.elements.get(0) != null) {
            if (key.compareTo(node.elements.get(0).key) <= 0) {
                return findLeaf(node.leftMostPtr, key);
            }
        } else {
            throw new RuntimeException("Empty node?!");
        }

        // TODO: Make this a binary search.
        for (int i = 1; i < node.elements.size(); i++) {
            if (key.compareTo(node.elements.get(i).key) <= 0) {
                return findLeaf(node.elements.get(i-1).rightPtr, key);
            }
        }
        return findLeaf(node.elements.get(node.elements.size() - 1).rightPtr, key);
    }

    public V get(K key) {
        return findEntryInLeaf(findLeaf(root, key), key);
    }

    public void printNode(Node<K> node, int level) {
        if (node == null)
            return;

        if (node.leafNode) {
            log.info("Level: {} ", level);
            node.print();
        }
        printNode(node.leftMostPtr, level+1);
        for (int i = 0; i < node.elements.size(); i++) {
            if (node.elements.get(i).rightPtr != null) {
                log.info("Level: {} {}, ", level, node.elements.get(i).key);
                printNode(node.elements.get(i).rightPtr, level+1);
            }
        }
    }

    public void print() {
        log.info("---- THE TREE -----\n");
        printNode(root, 0);
        log.info("-------------------\n");
    }
}

package org.corfudb.universe.node.stress;

public interface Stress {

    /**
     * To stress CPU usage on a node.
     */
    void stressCPULoad();

    /**
     * To stress IO usage on a node.
     */
    void stressIOLoad();

    /**
     * To stress memory (RAM) usage on a node.
     */
    void stressMemoryLoad();

    /**
     * To stress disk usage on a node.
     */
    void stressDiskLoad();

    /**
     * To release the existing stress load on a node.
     */
    void releaseStress();
}

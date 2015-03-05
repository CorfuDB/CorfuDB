package org.corfudb.runtime.collections;

import java.util.List;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public interface IZooKeeper
{
    //synchronous
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException;
    public Stat exists(String path, boolean watcher) throws KeeperException, InterruptedException;
    public void delete(String path, int version) throws KeeperException;
    public Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException;
    public byte[] getData(String path, boolean watcher, Stat stat) throws KeeperException, InterruptedException;
    public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException;

    //Mutating async ops
    void create(String path, byte[] data, List<ACL> acl, CreateMode createMode, AsyncCallback.StringCallback cb, Object ctxt);
    public void delete(String path, int version, AsyncCallback.VoidCallback cb, Object ctx);
    public void setData(String path, byte[] data, int version, AsyncCallback.StatCallback cb, Object ctx);

    //Non-mutating async ops
    public void exists(String path, boolean watch, AsyncCallback.StatCallback cb, Object ctx);
    public void getData(String path, boolean watch, AsyncCallback.DataCallback cb, Object ctx);
    public void getChildren(String path, boolean watch, AsyncCallback.ChildrenCallback cb, Object ctx);


}


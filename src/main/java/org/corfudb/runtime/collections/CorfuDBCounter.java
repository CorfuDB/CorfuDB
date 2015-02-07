package org.corfudb.runtime.collections;

import org.corfudb.runtime.AbstractRuntime;
import org.corfudb.runtime.CorfuDBObject;
import org.corfudb.runtime.CorfuDBObjectCommand;

import java.io.Serializable;
import java.util.HashSet;

public class CorfuDBCounter extends CorfuDBObject
{
    //backing state of the counter
    int value;

    boolean optimizereads = false;

    public CorfuDBCounter(AbstractRuntime tTR, long toid)
    {
        super(tTR, toid);
        value = 0;
    }
    public void applyToObject(Object bs)
    {
        //System.out.println("dummyupcall");
        //System.out.println("CorfuDBCounter received upcall");
        CounterCommand cc = (CounterCommand)bs;
        if(optimizereads)
            lock(true);
        if(cc.getCmdType()==CounterCommand.CMD_DEC)
            value--;
        else if(cc.getCmdType()==CounterCommand.CMD_INC)
            value++;
        else if(cc.getCmdType()==CounterCommand.CMD_READ)
            cc.setReturnValue(value);
        else
        {
            throw new RuntimeException("Unrecognized command in stream!");
        }
        if(optimizereads)
            unlock(true);
        //System.out.println("Counter value is " + value);
    }
    public void increment()
    {
        HashSet<Long> H = new HashSet<Long>(); H.add(this.getID());
        TR.update_helper(this, new CounterCommand(CounterCommand.CMD_INC));
    }
    public int read_optimized()
    {
        TR.query_helper(this);
        //what if the value changes between queryhelper and the actual read?
        //in the linearizable case, we are safe because we see a later version that strictly required
        //in the transactional case, the tx will spuriously abort, but safety will not be violated...
        //todo: is there a more elegant API?
        lock(false);
        int ret = value;
        unlock(false);
        return ret;
    }
    public int read()
    {
        if(optimizereads)
            return read_optimized();
        CounterCommand readcmd = new CounterCommand(CounterCommand.CMD_READ);
        TR.query_helper(this, null, readcmd);
        return (Integer)readcmd.getReturnValue();
    }
}

class CounterCommand extends CorfuDBObjectCommand
{
    int cmdtype;
    static final int CMD_DEC = 0;
    static final int CMD_INC = 1;
    static final int CMD_READ = 2;
    public CounterCommand(int tcmdtype)
    {
        cmdtype = tcmdtype;
    }
    public int getCmdType()
    {
        return cmdtype;
    }
};


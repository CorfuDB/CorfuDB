package org.corfudb.runtime.collections;

import org.corfudb.runtime.AbstractRuntime;
import org.corfudb.runtime.CorfuDBObject;

import java.io.Serializable;
import java.util.HashSet;

public class CorfuDBCounter extends CorfuDBObject
{
    //backing state of the counter
    int value;


    public CorfuDBCounter(AbstractRuntime tTR, long toid)
    {
        super(tTR, toid);
        value = 0;
        TR = tTR;
        oid = toid;
        TR.registerObject(this);
    }
    public void apply(Object bs)
    {
        //System.out.println("dummyupcall");
        System.out.println("CorfuDBCounter received upcall");
        CounterCommand cc = (CounterCommand)bs;
        lock(true);
        if(cc.getCmdType()==CounterCommand.CMD_DEC)
            value--;
        else if(cc.getCmdType()==CounterCommand.CMD_INC)
            value++;
        else
        {
            unlock(true);
            throw new RuntimeException("Unrecognized command in stream!");
        }
        unlock(true);
        System.out.println("Counter value is " + value);
    }
    public void increment()
    {
        HashSet<Long> H = new HashSet<Long>(); H.add(this.getID());
        TR.update_helper(this, new CounterCommand(CounterCommand.CMD_INC));
    }
    public int read()
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

}

class CounterCommand implements Serializable
{
    int cmdtype;
    static final int CMD_DEC = 0;
    static final int CMD_INC = 1;
    public CounterCommand(int tcmdtype)
    {
        cmdtype = tcmdtype;
    }
    public int getCmdType()
    {
        return cmdtype;
    }
};
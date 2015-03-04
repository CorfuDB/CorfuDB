package org.corfudb.runtime;

public class TLSCounter extends ThreadLocal<Integer>  {

    public int getval() {
        Integer val = get();
        if(val == null) {
            set(0);
            return 0;
        }
        return val;
    }

}



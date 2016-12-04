package org.corfudb.runtime.object;

/**
 * Created by dmalkhi on 12/4/16.
 */
public class CorfuSharedCounter {
    int value;

    public int getValue() { return value; }

    public void setValue(int newValue) { value = newValue; }

    @MutatorAccessor
    public int CAS(int testValue, int newValue) {
        int curValue = value;
        if (curValue == testValue)
            value = newValue;
        return curValue;
    }

}

package org.corfudb.logreplication.fsm;

import java.util.Observable;

public class ObservableValue extends Observable
{
    private int n = 0;
    public ObservableValue(int n)
    {
        this.n = n;
    }
    public void setValue(int n)
    {
        this.n = n;
        setChanged();
        notifyObservers();
    }
    public int getValue()
    {
        return n;
    }
}
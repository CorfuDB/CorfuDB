package org.corfudb.common.util;

import java.util.Observable;

/**
 * This class represents an observable value of type int, i.e.,
 * an object that the application will observe
 * to receive notifications on change.
 */
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
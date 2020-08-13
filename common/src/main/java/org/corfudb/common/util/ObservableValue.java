package org.corfudb.common.util;

import java.util.Observable;

/**
 * This class represents an observable value of type int, i.e.,
 * an object that the application will mark as observable, in order
 * to receive notifications on change.
 *
 * This is used to block and control tests.
 */
public class ObservableValue extends Observable
{
    private int n;

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

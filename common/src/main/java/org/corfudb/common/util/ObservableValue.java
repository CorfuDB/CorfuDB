package org.corfudb.common.util;

import java.util.Observable;

/**
 * This class represents an observable value of type int, i.e.,
 * an object that the application will mark as observable, in order
 * to receive notifications on change.
 *
 * This is used to block and control tests.
 */
public class ObservableValue<T> extends Observable
{
    private T n;

    public ObservableValue(T n)
    {
        this.n = n;
    }

    public void setValue(T n)
    {
        this.n = n;
        setChanged();
        notifyObservers();
    }

    public T getValue()
    {
        return n;
    }
}

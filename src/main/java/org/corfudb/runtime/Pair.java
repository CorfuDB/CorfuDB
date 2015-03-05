package org.corfudb.runtime;
import java.io.Serializable;

//todo: custom serialization + unit tests
public class Pair<X, Y> implements Serializable
{
    public final X first;
    public final Y second;
    public Pair(X f, Y s)
    {
        first = f;
        second = s;
    }

    public boolean equals(Pair<X,Y> otherP)
    {
        if(otherP==null) return false;
        if(((first==null && otherP.first==null) || (first!=null && first.equals(otherP.first))) //first matches up
                && ((second==null && otherP.second==null) || (second!=null && (second.equals(otherP.second))))) //second matches up
            return true;
        return false;
    }
    public String toString()
    {
        return "(" + first + "," + second + ")";
    }
}

package org.corfudb.runtime.smr;

import java.io.Serializable;

//todo: custom serialization + unit tests
public class Triple<X,Y,Z> implements Serializable
{
    public final X first;
    public final Y second;
    public final Z third;
    public Triple(X f, Y s, Z t)
    {
        first = f;
        second = s;
        third = t;
    }

    public boolean equals(Triple<X,Y,Z> otherT)
    {
        if(otherT==null) return false;
        if((((first==null && otherT.first==null)) || (first!=null && first.equals(otherT.first))) //first matches up
                && (((second==null && otherT.second==null)) || (second!=null && second.equals(otherT.second))) //second matches up
                && (((second==null && otherT.second==null)) || (second!=null && second.equals(otherT.second)))) //third matches up
            return true;
        return false;
    }
    public String toString()
    {
        return "(" + first + ", " + second + ", " + third + ")";
    }
}

package org.corfudb.runtime.object;

import jdk.nashorn.internal.objects.annotations.Constructor;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.Mutator;

/**
 * Created by dmalkhi on 12/2/16.
 */
@CorfuObject
public class CorfuCompoundObj {

    public class Inner {
        @Setter @Getter
        String firstName, lastName;

    }

    @Setter
    Inner user;

    @Setter
    int ID;

    @Mutator
    public void set(Inner in, int id) {
        setUser(in);
        setID(id);
    }

    @Accessor
    public Inner getUser() { return user;}

    @Accessor
    public int getID() { return ID;}
}

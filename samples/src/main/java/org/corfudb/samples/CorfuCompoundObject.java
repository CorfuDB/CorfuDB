package org.corfudb.samples;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.Mutator;

/**
 * Corfu objects may be compound, and work as expected.
 * Here is a simple example of a compound Corfu class.
 *
 * Created by dmalkhi on 1/5/17.
 */
@CorfuObject
public class CorfuCompoundObject {

    public class Inner {
        @Setter
        @Getter
        String firstName, lastName;

        public Inner(String f, String l) { firstName = f; lastName = l; }
    }

    Inner user;
    int ID;

    @Mutator(name = "set")
    public void set(Inner in, int id) {
        this.user = in;
        this.ID = id;
    }

    @Accessor
    public Inner getUser() { return user;}

    @Accessor
    public int getID() { return ID;}
}

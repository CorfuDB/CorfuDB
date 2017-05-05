package org.corfudb.runtime.object;

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

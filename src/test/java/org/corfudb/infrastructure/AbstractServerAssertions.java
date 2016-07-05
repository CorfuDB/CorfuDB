package org.corfudb.infrastructure;

import org.assertj.core.api.AbstractAssert;

/**
 * Created by mwei on 6/29/16.
 */
public class AbstractServerAssertions extends AbstractAssert<AbstractServerAssertions, AbstractServer> {

    public AbstractServerAssertions(AbstractServer actual) {
        super(actual, AbstractServerAssertions.class);
    }

    public static AbstractServerAssertions assertThat(AbstractServer actual) {
        return new AbstractServerAssertions(actual);
    }


}

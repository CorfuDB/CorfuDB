package org.corfudb.runtime.view;

import org.corfudb.runtime.view.TableRegistry.FullyQualifiedTableName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FullyQualifiedTableNameTest {

    @Test
    public void testToFqdn() {
        var tableFqdn = FullyQualifiedTableName.build("ns", "t1").toFqdn();
        assertEquals("ns$t1", tableFqdn);
    }
}

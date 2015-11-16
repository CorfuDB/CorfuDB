package org.corfudb.runtime.view;

import org.junit.Before;
import org.junit.Test;

import javax.json.Json;
import javax.json.JsonObject;

import static com.github.marschall.junitlambda.LambdaAssert.assertRaises;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dalia on 11/15/15.
 */
public class BareViewTest {

    JsonObject bootstrapView;
    CorfuDBView view;

    @Before
    void Initialize() {
        bootstrapView = Json.createObjectBuilder()
                .add ("configmaster",
                        Json.createArrayBuilder()
                                .add("cdbls://localhost:9999")
                                .build()
                )
                .build();
        view = new CorfuDBView(bootstrapView);
    }

    @Test
    void TestView() {
        assertThat(view)
                .isNotNull();
        assertThat(view.getLayouts().get(0))
                .isNotNull();
    }
}

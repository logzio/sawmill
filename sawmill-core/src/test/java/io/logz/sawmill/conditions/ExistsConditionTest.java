package io.logz.sawmill.conditions;

import io.logz.sawmill.Doc;
import org.junit.Test;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by naorguetta on 20/12/2016.
 */
public class ExistsConditionTest {

    @Test
    public void testFieldNotExists() {
        String field = "field1";
        ExistsCondition existsCondition = new ExistsCondition(field);

        Doc doc = createDoc("field2", "value2");
        assertThat(existsCondition.evaluate(doc)).isFalse();
    }

    @Test
    public void testFieldExists() {
        String field = "field1";
        ExistsCondition existsCondition = new ExistsCondition(field);

        Doc doc = createDoc("field1", "value2");
        assertThat(existsCondition.evaluate(doc)).isTrue();
    }
}
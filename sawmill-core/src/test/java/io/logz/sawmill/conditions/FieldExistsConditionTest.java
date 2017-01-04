package io.logz.sawmill.conditions;

import io.logz.sawmill.Doc;
import org.junit.Test;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class FieldExistsConditionTest {

    @Test
    public void testFieldNotExists() {
        String field = "field1";
        FieldExistsCondition fieldExistsCondition = new FieldExistsCondition(field);

        Doc doc = createDoc("field2", "value2");
        assertThat(fieldExistsCondition.evaluate(doc)).isFalse();
    }

    @Test
    public void testFieldExists() {
        String field = "field1";
        FieldExistsCondition fieldExistsCondition = new FieldExistsCondition(field);

        Doc doc = createDoc("field1", "value2");
        assertThat(fieldExistsCondition.evaluate(doc)).isTrue();
    }
}
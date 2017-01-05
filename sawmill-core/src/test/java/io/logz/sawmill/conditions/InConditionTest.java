package io.logz.sawmill.conditions;

import io.logz.sawmill.Doc;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class InConditionTest {

    @Test
    public void testEmptyList() {
        String field = "field1";
        String value = "value1";

        InCondition inCondition = new InCondition(field, value);

        Doc doc = createDoc("field1", Collections.emptyList());

        assertThat(inCondition.evaluate(doc)).isFalse();
    }

    @Test
    public void testNonList() {
        String field = "field1";
        String value = "value1";

        InCondition inCondition = new InCondition(field, value);

        Doc doc = createDoc("field1", "value2");

        assertThat(inCondition.evaluate(doc)).isFalse();
    }

    @Test
    public void testFieldNotExists() {
        String field = "field1";
        String value = "value1";

        InCondition inCondition = new InCondition(field, value);

        Doc doc = createDoc("field2", "value2");

        assertThat(inCondition.evaluate(doc)).isFalse();
    }

    @Test
    public void testStringList() {
        String field = "field1";
        String value = "value1";

        InCondition inCondition = new InCondition(field, value);

        Doc doc = createDoc("field1", Arrays.asList("value1", "value2"));

        assertThat(inCondition.evaluate(doc)).isTrue();
    }

}
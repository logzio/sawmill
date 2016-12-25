package io.logz.sawmill.conditions;

import io.logz.sawmill.Doc;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by naorguetta on 20/12/2016.
 */
public class FieldHasValueConditionTest {

    @Test
    public void testEmptyPossibleValues() {
        String field = "field1";
        List<String> possibleValues = Collections.emptyList();
        FieldHasValueCondition fieldHasValueCondition = new FieldHasValueCondition(field, possibleValues);

        Doc doc = createDoc("field1", "value1");
        assertThat(fieldHasValueCondition.evaluate(doc)).isFalse();
    }

    @Test
    public void testFieldNotExists() {
        String field = "field1";
        List<String> possibleValues = Arrays.asList("value1");
        FieldHasValueCondition fieldHasValueCondition = new FieldHasValueCondition(field, possibleValues);

        Doc doc = createDoc("field2", "value2");
        assertThat(fieldHasValueCondition.evaluate(doc)).isFalse();
    }

    @Test
    public void testFieldHasValue() {
        String field = "field1";
        List<String> possibleValues = Arrays.asList("value1", "value2");
        FieldHasValueCondition fieldHasValueCondition = new FieldHasValueCondition(field, possibleValues);

        Doc doc = createDoc("field1", "value1");
        assertThat(fieldHasValueCondition.evaluate(doc)).isTrue();
    }

}
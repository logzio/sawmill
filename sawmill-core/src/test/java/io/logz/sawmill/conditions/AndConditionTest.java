package io.logz.sawmill.conditions;

import io.logz.sawmill.Doc;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by naorguetta on 20/12/2016.
 */
public class AndConditionTest {

    @Test
    public void testEmptyConditions() {
        AndCondition andCondition = new AndCondition(Collections.emptyList());

        Doc doc = createDoc("field1", "value1");
        assertThat(andCondition.evaluate(doc)).isTrue();
    }

    @Test
    public void testOneConditionFalse() {
        AndCondition andCondition = new AndCondition(Collections.singletonList(c -> false));

        Doc doc = createDoc("field1", "value1");
        assertThat(andCondition.evaluate(doc)).isFalse();
    }

    @Test
    public void testTwoConditionsTrue() {
        AndCondition andCondition = new AndCondition(Arrays.asList(c -> true, c -> true));

        Doc doc = createDoc("field1", "value1");
        assertThat(andCondition.evaluate(doc)).isTrue();
    }

    @Test
    public void testTwoConditionsFalse() {
        AndCondition andCondition = new AndCondition(Arrays.asList(c -> true, c -> false));

        Doc doc = createDoc("field1", "value1");
        assertThat(andCondition.evaluate(doc)).isFalse();
    }

}
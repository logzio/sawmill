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
public class OrConditionTest {

    @Test
    public void testEmptyConditions() {
        OrCondition orCondition = new OrCondition(Collections.emptyList());

        Doc doc = createDoc("field1", "value1");
        assertThat(orCondition.evaluate(doc)).isFalse();
    }

    @Test
    public void testOneConditionFalse() {
        OrCondition andCondition = new OrCondition(Collections.singletonList(c -> true));

        Doc doc = createDoc("field1", "value1");
        assertThat(andCondition.evaluate(doc)).isTrue();
    }

    @Test
    public void testTwoConditionsTrue() {
        OrCondition andCondition = new OrCondition(Arrays.asList(c -> true, c -> false));

        Doc doc = createDoc("field1", "value1");
        assertThat(andCondition.evaluate(doc)).isTrue();
    }

    @Test
    public void testTwoConditionsFalse() {
        OrCondition andCondition = new OrCondition(Arrays.asList(c -> false, c -> false));

        Doc doc = createDoc("field1", "value1");
        assertThat(andCondition.evaluate(doc)).isFalse();
    }

}
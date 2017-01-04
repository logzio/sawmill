package io.logz.sawmill.conditions;

import io.logz.sawmill.Doc;
import org.junit.Test;

import java.util.Collections;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class NotConditionTest {

    @Test
    public void testNotFalse() {
        NotCondition notCondition = new NotCondition(Collections.singletonList(c -> false));
        Doc doc = createDoc("field1", "value1");

        assertThat(notCondition.evaluate(doc)).isTrue();
    }

    @Test
    public void testNotTrue() {
        NotCondition notCondition = new NotCondition(Collections.singletonList(c -> true));
        Doc doc = createDoc("field1", "value1");

        assertThat(notCondition.evaluate(doc)).isFalse();
    }

}
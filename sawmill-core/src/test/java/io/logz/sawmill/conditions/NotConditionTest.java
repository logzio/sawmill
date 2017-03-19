package io.logz.sawmill.conditions;

import io.logz.sawmill.Doc;
import org.junit.Test;

import java.util.Collections;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    public void testInvalidNotConfiguration() {
        NotCondition.Factory factory = new NotCondition.Factory();
        assertThatThrownBy(() -> factory.create(emptyMap(), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'Not' condition must contain a valid list of conditions, with at least one condition");
    }

}
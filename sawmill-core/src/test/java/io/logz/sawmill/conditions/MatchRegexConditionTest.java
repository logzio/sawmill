package io.logz.sawmill.conditions;

import io.logz.sawmill.Doc;
import org.junit.Test;

import java.util.regex.PatternSyntaxException;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MatchRegexConditionTest {

    @Test
    public void testMatchingValue() {
        String field = "field1";
        String regex = "^Hello.+";
        MatchRegexCondition matchRegexCondition = new MatchRegexCondition(field, regex);

        Doc doc = createDoc("field1", "Hello World!");
        assertThat(matchRegexCondition.evaluate(doc)).isTrue();
    }

    @Test
    public void testNotMatchingValue() {
        String field = "field1";
        String regex = "Wed.+";
        MatchRegexCondition matchRegexCondition = new MatchRegexCondition(field, regex);

        Doc doc = createDoc("field1", "Thursday");
        assertThat(matchRegexCondition.evaluate(doc)).isFalse();
    }

    @Test
    public void testInvalidRegex() {
        String field = "field1";
        String regex = "Wed\\"+'x';
        assertThatThrownBy(() -> new MatchRegexCondition(field, regex))
                .isInstanceOf(PatternSyntaxException.class)
                .hasMessageContaining("Illegal hexadecimal escape sequence near index");
    }

    @Test
    public void testWrongValueType() {
        String field = "field1";
        String regex = "Wed.+";
        MatchRegexCondition matchRegexCondition = new MatchRegexCondition(field, regex);

        Doc doc = createDoc("field1", 123);
        assertThat(matchRegexCondition.evaluate(doc)).isFalse();
    }

}
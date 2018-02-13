package io.logz.sawmill.conditions;

import io.logz.sawmill.ConditionFactoryRegistry;
import io.logz.sawmill.Doc;
import io.logz.sawmill.parser.ConditionParser;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.regex.PatternSyntaxException;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MatchRegexConditionTest {
    private ConditionParser conditionParser;

    @Before
    public void init() {
        ConditionFactoryRegistry conditionFactoryRegistry = ConditionFactoryRegistry.getInstance();
        conditionParser = new ConditionParser(conditionFactoryRegistry);
    }

    @Test
    public void testMatchingValueCaseSensitive() {
        String field = "field1";
        String regex = "^Hello.+";
        Map<String, Object> config = createConfig("field", field,
                "regex", regex,
                "matchPartOfValue", false);
        MatchRegexCondition matchRegexCondition = new MatchRegexCondition.Factory().create(config, conditionParser);

        Doc doc = createDoc("field1", "Hello World!");
        assertThat(matchRegexCondition.evaluate(doc)).isTrue();

        doc = createDoc("field1", "hello World!");
        assertThat(matchRegexCondition.evaluate(doc)).isFalse();
    }

    @Test
    public void testMatchingValueCaseInsensitive() {
        String field = "field1";
        String regex = "^Hello.+";
        Map<String, Object> config = createConfig("field", field,
                "regex", regex,
                "caseInsensitive", true,
                "matchPartOfValue", false);
        MatchRegexCondition matchRegexCondition = new MatchRegexCondition.Factory().create(config, conditionParser);

        Doc doc = createDoc("field1", "Hello World!");
        assertThat(matchRegexCondition.evaluate(doc)).isTrue();

        doc = createDoc("field1", "hello World!");
        assertThat(matchRegexCondition.evaluate(doc)).isTrue();
    }

    @Test
    public void testMatchingPartOfValue() {
        String field = "field1";
        String regex = "World";
        Map<String, Object> config = createConfig("field", field,
                "regex", regex,
                "matchPartOfValue", false);
        MatchRegexCondition matchRegexCondition = new MatchRegexCondition.Factory().create(config, conditionParser);

        Doc doc = createDoc("field1", "Hello World!");
        assertThat(matchRegexCondition.evaluate(doc)).isFalse();

        config.remove("matchPartOfValue");
        MatchRegexCondition matchPartOfValueRegexCondition = new MatchRegexCondition.Factory().create(config, conditionParser);
        doc = createDoc("field1", "hello World!");
        assertThat(matchPartOfValueRegexCondition.evaluate(doc)).isTrue();
    }

    @Test
    public void testMatchingPartOfValueCaseInsensitive() {
        String field = "field1";
        String regex = "world";
        Map<String, Object> config = createConfig("field", field,
                "regex", regex);
        MatchRegexCondition matchRegexCondition = new MatchRegexCondition.Factory().create(config, conditionParser);

        Doc doc = createDoc("field1", "Hello World!");
        assertThat(matchRegexCondition.evaluate(doc)).isFalse();

        config.put("caseInsensitive", true);
        MatchRegexCondition matchPartOfValueRegexCondition = new MatchRegexCondition.Factory().create(config, conditionParser);
        doc = createDoc("field1", "hello World!");
        assertThat(matchPartOfValueRegexCondition.evaluate(doc)).isTrue();
    }

    @Test
    public void testNotMatchingValue() {
        String field = "field1";
        String regex = "Wed.+";
        Map<String, Object> config = createConfig("field", field,
                "regex", regex,
                "caseInsensitive", false,
                "matchPartOfValue", false);
        MatchRegexCondition matchRegexCondition = new MatchRegexCondition.Factory().create(config, conditionParser);

        Doc doc = createDoc("field1", "Thursday");
        assertThat(matchRegexCondition.evaluate(doc)).isFalse();
    }

    @Test
    public void testInvalidRegex() {
        String field = "field1";
        String invalidRegexEscaping = "Wed\\"+'x';
        assertThatThrownBy(() -> new MatchRegexCondition(field, invalidRegexEscaping, false, false))
                .isInstanceOf(PatternSyntaxException.class)
                .hasMessageContaining("Illegal hexadecimal escape sequence near index");

        String invalidRegex = "[]";
        assertThatThrownBy(() -> new MatchRegexCondition(field, invalidRegex, false, false))
                .isInstanceOf(PatternSyntaxException.class)
                .hasMessageContaining("Unclosed character class near index");
    }

    @Test
    public void testWrongValueType() {
        String field = "field1";
        String regex = "Wed.+";
        Map<String, Object> config = createConfig("field", field,
                "regex", regex,
                "caseInsensitive", false,
                "matchPartOfValue", false);
        MatchRegexCondition matchRegexCondition = new MatchRegexCondition.Factory().create(config, conditionParser);

        Doc doc = createDoc("field1", 123);
        assertThat(matchRegexCondition.evaluate(doc)).isFalse();
    }

}
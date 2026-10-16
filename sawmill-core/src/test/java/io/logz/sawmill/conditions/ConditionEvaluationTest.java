package io.logz.sawmill.conditions;

import io.logz.sawmill.Condition;
import io.logz.sawmill.Doc;
import io.logz.sawmill.Template;
import io.logz.sawmill.TemplateService;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class ConditionEvaluationTest {

    private static final Condition TRUE = doc -> true;
    private static final Condition FALSE = doc -> false;
    private static final Condition UNREACHABLE = doc -> {
        throw new AssertionError("evaluated after the result was known");
    };

    @Test
    public void testHasValueMatchesPreviousEvaluation() {
        TemplateService templates = new TemplateService();
        Object[] pool = {"a", "b", "200", "true", 200L, 1.5d, 2.5d, true, false, null, "{{other}}", "x{{other}}", "y"};
        Object[] docValues = {"a", "200", 200, 200L, 1.5d, 2.5f, true, "xq", "q", Arrays.asList("a"), null};
        Random random = new Random(29);

        for (int n = 0; n < 20000; n++) {
            List<Object> possibleValues = new ArrayList<>();
            for (int i = random.nextInt(6); i > 0; i--) {
                Object value = pool[random.nextInt(pool.length)];
                // as the factory builds them: strings become templates
                possibleValues.add(value instanceof String ? templates.createTemplate((String) value) : value);
            }
            Map<String, Object> source = new HashMap<>();
            source.put("other", random.nextBoolean() ? "q" : "z");
            if (random.nextInt(5) > 0) source.put("f", docValues[random.nextInt(docValues.length)]);
            Doc doc = new Doc(source);

            boolean expected = previousEvaluation("f", possibleValues, doc);
            assertThat(new FieldHasValueCondition("f", possibleValues).evaluate(doc))
                    .as("%s in %s", source.get("f"), possibleValues).isEqualTo(expected);
        }
    }

    @Test
    public void testAndOrNotStopAtTheFirstDecidingCondition() {
        assertThat(new AndCondition(Arrays.asList(FALSE, UNREACHABLE)).evaluate(doc())).isFalse();
        assertThat(new AndCondition(Arrays.asList(TRUE, TRUE)).evaluate(doc())).isTrue();
        assertThat(new AndCondition(Collections.emptyList()).evaluate(doc())).isTrue();
        assertThat(new OrCondition(Arrays.asList(TRUE, UNREACHABLE)).evaluate(doc())).isTrue();
        assertThat(new OrCondition(Arrays.asList(FALSE, FALSE)).evaluate(doc())).isFalse();
        assertThat(new OrCondition(Collections.emptyList()).evaluate(doc())).isFalse();
        assertThat(new NotCondition(Arrays.asList(TRUE, UNREACHABLE)).evaluate(doc())).isFalse();
        assertThat(new NotCondition(Arrays.asList(FALSE, FALSE)).evaluate(doc())).isTrue();
        assertThat(new NotCondition(Collections.emptyList()).evaluate(doc())).isTrue();
    }

    /** FieldHasValueCondition.evaluate as it was, with a stream per evaluation. */
    private static boolean previousEvaluation(String field, List<Object> possibleValues, Doc doc) {
        if (!doc.hasField(field)) return false;
        Object value = doc.getField(field);
        if (value instanceof Float) value = ((Float) value).doubleValue();
        else if (value instanceof Integer) value = ((Integer) value).longValue();
        Object finalValue = value;
        return possibleValues.stream()
                .map(possibleValue -> possibleValue instanceof Template ? ((Template) possibleValue).render(doc) : possibleValue)
                .anyMatch(finalValue::equals);
    }

    private static Doc doc() {
        Map<String, Object> source = new HashMap<>();
        source.put("message", "m");
        return new Doc(source);
    }
}

package io.logz.sawmill.conditions;

import io.logz.sawmill.ConditionFactoryRegistry;
import io.logz.sawmill.ConditionalFactoriesLoader;
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

public class MathComparatorConditionTest {

    public ConditionParser conditionParser;

    @Before
    public void init() {
        ConditionFactoryRegistry conditionFactoryRegistry = new ConditionFactoryRegistry();
        ConditionalFactoriesLoader.getInstance().loadAnnotatedProcessors(conditionFactoryRegistry);
        conditionParser = new ConditionParser(conditionFactoryRegistry);
    }

    @Test
    public void testGte() {
        String field = "field1";
        Long gte = 10l;


        Map<String, Object> config = createConfig("field", field,
                "gte", gte);
        MathComparatorCondition mathComparatorCondition = new MathComparatorCondition.Factory().create(config, conditionParser);

        Doc doc = createDoc("field1", 15l);
        assertThat(mathComparatorCondition.evaluate(doc)).isTrue();

        doc = createDoc("field1", 10);
        assertThat(mathComparatorCondition.evaluate(doc)).isTrue();

        doc = createDoc("field1", 5l);
        assertThat(mathComparatorCondition.evaluate(doc)).isFalse();
    }

    @Test
    public void testGt() {
        String field = "field1";
        Long gt = 10l;


        Map<String, Object> config = createConfig("field", field,
                "gt", gt);
        MathComparatorCondition mathComparatorCondition = new MathComparatorCondition.Factory().create(config, conditionParser);

        Doc doc = createDoc("field1", 15l);
        assertThat(mathComparatorCondition.evaluate(doc)).isTrue();

        doc = createDoc("field1", 10);
        assertThat(mathComparatorCondition.evaluate(doc)).isFalse();

        doc = createDoc("field1", 5l);
        assertThat(mathComparatorCondition.evaluate(doc)).isFalse();
    }

    @Test
    public void testLte() {
        String field = "field1";
        Long lte = 10l;


        Map<String, Object> config = createConfig("field", field,
                "lte", lte);
        MathComparatorCondition mathComparatorCondition = new MathComparatorCondition.Factory().create(config, conditionParser);

        Doc doc = createDoc("field1", 15l);
        assertThat(mathComparatorCondition.evaluate(doc)).isFalse();

        doc = createDoc("field1", 10);
        assertThat(mathComparatorCondition.evaluate(doc)).isTrue();

        doc = createDoc("field1", 5l);
        assertThat(mathComparatorCondition.evaluate(doc)).isTrue();
    }

    @Test
    public void testLt() {
        String field = "field1";
        Long lt = 10l;


        Map<String, Object> config = createConfig("field", field,
                "lt", lt);
        MathComparatorCondition mathComparatorCondition = new MathComparatorCondition.Factory().create(config, conditionParser);

        Doc doc = createDoc("field1", 15l);
        assertThat(mathComparatorCondition.evaluate(doc)).isFalse();

        doc = createDoc("field1", 10);
        assertThat(mathComparatorCondition.evaluate(doc)).isFalse();

        doc = createDoc("field1", 5l);
        assertThat(mathComparatorCondition.evaluate(doc)).isTrue();
    }

    @Test
    public void testGteAndLte() {
        String field = "field1";
        Long lte = 20l;
        Long gte = 10l;


        Map<String, Object> config = createConfig("field", field,
                "lte", lte,
                "gte", gte);
        MathComparatorCondition mathComparatorCondition = new MathComparatorCondition.Factory().create(config, conditionParser);

        Doc doc = createDoc("field1", 15l);
        assertThat(mathComparatorCondition.evaluate(doc)).isTrue();

        doc = createDoc("field1", 10);
        assertThat(mathComparatorCondition.evaluate(doc)).isTrue();

        doc = createDoc("field1", 20);
        assertThat(mathComparatorCondition.evaluate(doc)).isTrue();

        doc = createDoc("field1", 5l);
        assertThat(mathComparatorCondition.evaluate(doc)).isFalse();

        doc = createDoc("field1", 25l);
        assertThat(mathComparatorCondition.evaluate(doc)).isFalse();
    }

    @Test
    public void testGtAndLt() {
        String field = "field1";
        Long lt = 20l;
        Long gt = 10l;


        Map<String, Object> config = createConfig("field", field,
                "lt", lt,
                "gt", gt);
        MathComparatorCondition mathComparatorCondition = new MathComparatorCondition.Factory().create(config, conditionParser);

        Doc doc = createDoc("field1", 15l);
        assertThat(mathComparatorCondition.evaluate(doc)).isTrue();

        doc = createDoc("field1", 10);
        assertThat(mathComparatorCondition.evaluate(doc)).isFalse();

        doc = createDoc("field1", 20);
        assertThat(mathComparatorCondition.evaluate(doc)).isFalse();

        doc = createDoc("field1", 5l);
        assertThat(mathComparatorCondition.evaluate(doc)).isFalse();

        doc = createDoc("field1", 25l);
        assertThat(mathComparatorCondition.evaluate(doc)).isFalse();
    }
}
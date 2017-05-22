package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import org.junit.Test;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;

public class MathProcessorTest {

    @Test
    public void testExpressionWithFields() {
        String targetField = "sum";
        String expression = "{{field1}} + {{field2}} * 10 / ((5 + {{field3}})^2)";
        Doc doc = createDoc("field1", 5,
                "field2", 10.5d,
                "field3", 7l);

        MathProcessor mathProcessor = createProcessor(MathProcessor.class, "expression", expression, "targetField", targetField);

        ProcessResult processResult = mathProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((Double) doc.getField(targetField)).isEqualTo(5 + 10.5d * 10 / ((5 + 7)^2));
    }

    @Test
    public void testValidExpression() {
        String targetField = "sum";
        String expression = "5 + 10.5 * 10 / (5 + 7)";
        Doc doc = createDoc("field1", 5,
                "field2", 10.5d,
                "field3", 7l);

        MathProcessor mathProcessor = createProcessor(MathProcessor.class, "expression", expression, "targetField", targetField);

        ProcessResult processResult = mathProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((Double) doc.getField(targetField)).isEqualTo(5 + 10.5d * 10 / (5 + 7));
    }

    @Test
    public void testInvalidExpression() {
        String targetField = "sum";
        String expression = "5 + 10.5 * 10 / (5 + 7";
        Doc doc = createDoc("field1", 5,
                "field2", 10.5d,
                "field3", 7l);

        MathProcessor mathProcessor = createProcessor(MathProcessor.class, "expression", expression, "targetField", targetField);

        ProcessResult processResult = mathProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
    }

    @Test
    public void testJavaScriptExpression() {
        String targetField = "sum";
        String expression = "var i = [1,2,3]; i.forEach(function(v) { console.log(v + 1); });";
        Doc doc = createDoc("field1", 5);

        MathProcessor mathProcessor = createProcessor(MathProcessor.class, "expression", expression, "targetField", targetField);

        ProcessResult processResult = mathProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
    }
}

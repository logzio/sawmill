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
        String expression = "{{field1}} + {{field2}} * 10 / (5 + {{field3}})";
        Doc doc = createDoc("field1", 5,
                "field2", 10.5d,
                "field3", 7l);

        MathProcessor mathProcessor = createProcessor(MathProcessor.class, "expression", expression, "targetField", targetField);

        ProcessResult processResult = mathProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((Double) doc.getField(targetField)).isEqualTo(5 + 10.5d * 10 / (5 + 7));
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
    public void testInvalidExpressionWithFields() {
        String targetField = "sum";
        String expression = "{{field1}} + {{field2}} * 10 / (5 + {{field3}})";
        Doc doc = createDoc("field1", "five",
                "field2", 10.5d,
                "field3", 7l);

        MathProcessor mathProcessor = createProcessor(MathProcessor.class, "expression", expression, "targetField", targetField);

        ProcessResult processResult = mathProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
    }

    @Test
    public void testExpressionEvaluator() {
        MathProcessor.ExpressionEvaluator evaluator = new MathProcessor.ExpressionEvaluator();
        assertThat(evaluator.eval("5")).isEqualTo(5);
        assertThat(evaluator.eval("5.7")).isEqualTo(5.7);
        assertThat(evaluator.eval("(5.5)")).isEqualTo(5.5);
        assertThat(evaluator.eval("(5+1)")).isEqualTo(6);
        assertThat(evaluator.eval("(5.7-1.2)")).isEqualTo(4.5);
        assertThat(evaluator.eval(" ( 5.7 - 1.2 ) ")).isEqualTo(4.5);
        assertThat(evaluator.eval("(5.7*1.2)")).isEqualTo(5.7*1.2);
        assertThat(evaluator.eval("(6.6/6)")).isEqualTo(6.6/6);
        assertThat(evaluator.eval("(6.6/(2+4))")).isEqualTo(6.6/6);
        assertThat(evaluator.eval("(6.6/ ( 2 + (2*2) ))")).isEqualTo(6.6/6);
        assertThat(evaluator.eval("(5.7*1.2*3.4)")).isEqualTo(5.7*1.2*3.4);
        assertThat(evaluator.eval("(5.7 - 1.2) - 3.4")).isEqualTo((5.7) - (1.2) - (3.4));
        assertThat(evaluator.eval("((5.7-1.2) - 3.4)")).isEqualTo((5.7) - (1.2) - (3.4));
    }
}

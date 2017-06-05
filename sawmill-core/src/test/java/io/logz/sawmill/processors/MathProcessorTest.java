package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import org.junit.Test;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        String expression = "5 + 10.5 * 10 / ((5 + 7)^2)";
        Doc doc = createDoc("field1", 5,
                "field2", 10.5d,
                "field3", 7l);

        MathProcessor mathProcessor = createProcessor(MathProcessor.class, "expression", expression, "targetField", targetField);

        ProcessResult processResult = mathProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((Double) doc.getField(targetField)).isEqualTo(5 + 10.5d * 10 / Math.pow((5 + 7), 2));
    }

    @Test
    public void testInvalidExpression() {
        assertThatThrownBy(() -> createProcessor(MathProcessor.class,
                "expression", "5 + 10.5 * 10 / (5 + 7",
                "targetField", "sum")).isInstanceOf(ProcessorConfigurationException.class);
        assertThatThrownBy(() -> createProcessor(MathProcessor.class,
                "expression", "5 / five * (5 + 7)",
                "targetField", "sum")).isInstanceOf(ProcessorConfigurationException.class);
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
    public void testDividingByZero() {
        String targetField = "sum";
        Doc doc = createDoc("field1", 0);

        String expression = "5 / 0";
        MathProcessor mathProcessor = createProcessor(MathProcessor.class, "expression", expression, "targetField", targetField);
        ProcessResult processResult = mathProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();

        expression = "5 / {{field1}}";
        mathProcessor = createProcessor(MathProcessor.class, "expression", expression, "targetField", targetField);
        processResult = mathProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
    }

    @Test
    public void testAbsolute() {
        String targetField = "sum";
        Doc doc = createDoc("field1", 0);

        String expression = "abs(-300)";
        MathProcessor mathProcessor = createProcessor(MathProcessor.class, "expression", expression, "targetField", targetField);
        ProcessResult processResult = mathProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((Double) doc.getField(targetField)).isEqualTo(300);

        expression = "abs(300)";
        mathProcessor = createProcessor(MathProcessor.class, "expression", expression, "targetField", targetField);
        processResult = mathProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((Double) doc.getField(targetField)).isEqualTo(300);
    }
}

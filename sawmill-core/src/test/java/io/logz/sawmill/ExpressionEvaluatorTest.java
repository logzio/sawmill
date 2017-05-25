package io.logz.sawmill;

import io.logz.sawmill.utilities.ExpressionEvaluator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ExpressionEvaluatorTest {

    @Test
    public void testValidExpressions() {
        ExpressionEvaluator evaluator = new ExpressionEvaluator();
        assertThat(evaluator.calculate("5")).isEqualTo(5);
        assertThat(evaluator.calculate("5.7")).isEqualTo(5.7);
        assertThat(evaluator.calculate("(5.5)")).isEqualTo(5.5);
        assertThat(evaluator.calculate("(5+1)")).isEqualTo(6);
        assertThat(evaluator.calculate("(5.7-1.2)")).isEqualTo(4.5);
        assertThat(evaluator.calculate(" ( 5.7 - 1.2 ) ")).isEqualTo(4.5);
        assertThat(evaluator.calculate("(5.7*1.2)")).isEqualTo(5.7*1.2);
        assertThat(evaluator.calculate("(6.6/6)")).isEqualTo(6.6/6);
        assertThat(evaluator.calculate("(6.6/(2+4))")).isEqualTo(6.6/6);
        assertThat(evaluator.calculate("(6.6/ ( 2 + (2*2) ))")).isEqualTo(6.6/6);
        assertThat(evaluator.calculate("(5.7*1.2*3.4)")).isEqualTo(5.7*1.2*3.4);
        assertThat(evaluator.calculate("(5.7 - 1.2) - 3.4")).isEqualTo((5.7) - (1.2) - (3.4));
        assertThat(evaluator.calculate("((5.7-1.2) - 3.4)")).isEqualTo((5.7) - (1.2) - (3.4));
    }

    @Test
    public void testInvalidExpressions() {
        ExpressionEvaluator evaluator = new ExpressionEvaluator();
        assertThat(evaluator.calculate("5 5 + 2")).isEqualTo(null);
        assertThat(evaluator.calculate("5 + - 2")).isEqualTo(null);
        assertThat(evaluator.calculate("(+) 2")).isEqualTo(null);
        assertThat(evaluator.calculate("((5 + 2)")).isEqualTo(null);
    }
}

package io.logz.sawmill.utilities;

import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;

import java.util.Set;

public class MathExpressionProvider {
    private final ThreadLocal<Expression> localExpression;

    public MathExpressionProvider(String expression, Set<String> variables) {
        localExpression = ThreadLocal.withInitial(() -> {
            try {
                return new ExpressionBuilder(expression)
                        .variables(variables)
                        .build();
            } catch (IllegalArgumentException e) {
                throw new ProcessorConfigurationException(String.format("invalid expression [%s]", expression));
            }
        });
    }

    public Expression provide() {
        return localExpression.get();
    }
}

package io.logz.sawmill.processors;

import com.google.common.primitives.Doubles;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.utilities.JsonUtils;
import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import net.objecthunter.exp4j.ValidationResult;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "math", factory = MathProcessor.Factory.class)
public class MathProcessor implements Processor {
    private final String targetField;
    private final Expression expression;
    private final Set<String> variables;

    public MathProcessor(String targetField, Expression expression, Set<String> variables) {
        this.targetField = requireNonNull(targetField, "target field cannot be null");
        this.expression = requireNonNull(expression, "expression cannot be null");
        this.variables = variables;
    }

    @Override
    public ProcessResult process(Doc doc) {
        Map<String, Double> variablesMap = new HashMap<>();

        for (String variable : variables) {
            if (!doc.hasField(variable)) {
                return ProcessResult.failure("field [%s] is missing");
            }

            Double value = null;
            Object fieldValue = doc.getField(variable);
            if (fieldValue instanceof Number) {
                value = ((Number) fieldValue).doubleValue();
            } else if (fieldValue instanceof String) {
                value = Doubles.tryParse((String) fieldValue);
            }

            if (value == null) {
                return ProcessResult.failure("field [%s] is not a number");
            }

            variablesMap.put(variable, value);
        }

        expression.setVariables(variablesMap);

        try {
            doc.addField(targetField,  expression.evaluate());
        } catch (ArithmeticException e) {
            return ProcessResult.failure("Division by zero!");
        }

        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        private final Pattern mustachePattern = Pattern.compile("\\{\\{(.+?)\\}\\}");

        @Override
        public MathProcessor create(Map<String,Object> config) {
            MathProcessor.Configuration mathConfig = JsonUtils.fromJsonMap(MathProcessor.Configuration.class, config);

            Set<String> variables = findVariables(mathConfig.getExpression());

            Expression expression;
            try {
                 expression = new ExpressionBuilder(trimMustache(mathConfig.getExpression()))
                        .variables(variables)
                        .build();
            } catch (IllegalArgumentException e) {
                throw new ProcessorConfigurationException("invalid expression");
            }

            if (!expression.validate(false).isValid()) {
                throw new ProcessorConfigurationException("invalid expression");
            }

            return new MathProcessor(mathConfig.getTargetField(), expression, variables);
        }

        private String trimMustache(String expression) {
            return expression.replaceAll("(\\{\\{|\\}\\})", "");
        }

        private Set<String> findVariables(String expression) {
            Set<String> variables = new HashSet<>();
            Matcher matcher = mustachePattern.matcher(expression);

            while (matcher.find()) {
                variables.add(matcher.group(1));
            }

            return variables;
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String targetField;
        private String expression;

        public Configuration() { }

        public String getTargetField() {
            return targetField;
        }

        public String getExpression() {
            return expression;
        }
    }
}

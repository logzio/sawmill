package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.utilities.JsonUtils;
import io.logz.sawmill.utilities.MathExpressionProvider;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.logz.sawmill.FieldType.DOUBLE;
import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "math", factory = MathProcessor.Factory.class)
public class MathProcessor implements Processor {
    private final String targetField;
    private final MathExpressionProvider mathExpressionProvider;
    private final Set<String> variables;

    public MathProcessor(String targetField, MathExpressionProvider mathExpressionProvider, Set<String> variables) {
        this.targetField = requireNonNull(targetField, "target field cannot be null");
        this.mathExpressionProvider = requireNonNull(mathExpressionProvider, "expression cannot be null");
        this.variables = variables;
    }

    @Override
    public ProcessResult process(Doc doc) {
        Map<String, Double> variablesMap = new HashMap<>();

        for (String variable : variables) {
            if (!doc.hasField(variable)) {
                return ProcessResult.failure("field [%s] is missing");
            }

            Double value = resolveVariable(doc, variable);

            if (value == null) {
                return ProcessResult.failure("field [%s] is not a number");
            }

            variablesMap.put(variable, value);
        }

        mathExpressionProvider.provide().setVariables(variablesMap);

        try {
            doc.addField(targetField,  mathExpressionProvider.provide().evaluate());
        } catch (ArithmeticException e) {
            return ProcessResult.failure("Division by zero!");
        }

        return ProcessResult.success();
    }

    private Double resolveVariable(Doc doc, String variable) {
        Object fieldValue = doc.getField(variable);
        return (Double) DOUBLE.convertFrom(fieldValue);
    }

    public static class Factory implements Processor.Factory {
        private final Pattern mustachePattern = Pattern.compile("\\{\\{(.+?)\\}\\}");

        @Override
        public MathProcessor create(Map<String,Object> config) {
            MathProcessor.Configuration mathConfig = JsonUtils.fromJsonMap(MathProcessor.Configuration.class, config);

            String expression = requireNonNull(mathConfig.getExpression(), "expression cannot be null");
            Set<String> variables = findVariables(expression);

            MathExpressionProvider mathExpressionProvider = new MathExpressionProvider(trimMustache(expression), variables);

            if (!mathExpressionProvider.provide().validate(false).isValid()) {
                throw new ProcessorConfigurationException(String.format("invalid expression [%s]", expression));
            }

            return new MathProcessor(requireNonNull(mathConfig.getTargetField()), mathExpressionProvider, variables);
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

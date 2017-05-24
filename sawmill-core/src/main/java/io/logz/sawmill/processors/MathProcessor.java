package io.logz.sawmill.processors;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Doubles;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.Template;
import io.logz.sawmill.TemplateService;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

import javax.inject.Inject;
import java.util.Map;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "math", factory = MathProcessor.Factory.class)
public class MathProcessor implements Processor {
    private final String targetField;
    private final Template expressionTemplate;
    private ExpressionEvaluator expressionEvaluator;

    public MathProcessor(String targetField, Template expressionTemplate) {
        this.targetField = requireNonNull(targetField, "target field cannot be null");
        this.expressionTemplate = requireNonNull(expressionTemplate, "expression cannot be null");
        expressionEvaluator = new ExpressionEvaluator();
    }

    @Override
    public ProcessResult process(Doc doc) {
        String expression = expressionTemplate.render(doc);

        Double result = expressionEvaluator.eval(expression);

        if (result == null) {
            return ProcessResult.failure("invalid expression");
        }

        doc.addField(targetField, result);

        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        private final TemplateService templateService;

        @Inject
        public Factory(TemplateService templateService) {
            this.templateService = templateService;
        }

        @Override
        public MathProcessor create(Map<String,Object> config) {
            MathProcessor.Configuration mathConfig = JsonUtils.fromJsonMap(MathProcessor.Configuration.class, config);

            return new MathProcessor(mathConfig.getTargetField(), templateService.createTemplate(mathConfig.getExpression()));
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

    public static class ExpressionEvaluator {
        private final Map<Character, BiFunction<Double,Double,Double>> operationsMap = new ImmutableMap.Builder<Character, BiFunction<Double,Double,Double>>()
                .put('+', (a,b) -> a+b)
                .put('-', (a,b) -> a-b)
                .put('/', (a,b) -> a/b)
                .put('*', (a,b) -> a*b)
                .build();

        public Double eval(String expression) {
            String exp = trimParentheses(expression.trim());
            Double value = Doubles.tryParse(exp);
            if (value != null) return value;
            int operatorIndex = findOperationIndex(exp);
            if (operatorIndex == -1) {
                return null;
            }
            String leftExp = exp.substring(0, operatorIndex);
            String rightExp = exp.substring(operatorIndex+1, exp.length());
            char operation = exp.charAt(operatorIndex);
            if (!operationsMap.containsKey(operation)) {
                return null;
            }

            Double left = eval(leftExp);
            Double right = eval(rightExp);
            if (left == null || right == null) {
                return null;
            }
            return operationsMap.get(operation).apply(left, right);
        }
        private int findOperationIndex(String exp) {
            int i;
            for (i=0;i<exp.length();i++) {
                char charAt = exp.charAt(i);
                if (operationsMap.keySet().contains(charAt)) return i;
                if (charAt == '(') {
                    int parenthesesCount = 1;
                    i++;
                    while (parenthesesCount > 0 && i < exp.length()) {
                        char skippedCharAt = exp.charAt(i);
                        if (skippedCharAt == '(') {
                            parenthesesCount++;
                        } else if (skippedCharAt == ')') {
                            parenthesesCount--;
                        }
                        i++;
                    }
                }
            }
            if (i >= exp.length()) return -1;
            return i;
        }

        private String trimParentheses(String input) {
            if (input.charAt(0) == '(') {
                int closingParenthesisIndex = getClosingParenthesisIndex(input);
                if (closingParenthesisIndex == input.length()-1)
                    return trimParentheses(input.substring(1, input.length()-1));
            }
            return input;
        }
        private int getClosingParenthesisIndex(String input) {
            int parenthesesCount = 1;
            int i = 1;
            while (parenthesesCount > 0 && i < input.length()) {
                char skippedCharAt = input.charAt(i);
                if (skippedCharAt == '(') {
                    parenthesesCount++;
                } else if (skippedCharAt == ')') {
                    parenthesesCount--;
                }
                if (parenthesesCount == 0) return i;
                i++;
            }
            return -1; //invalid input string
        }
    }
}

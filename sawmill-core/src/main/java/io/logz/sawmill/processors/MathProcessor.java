package io.logz.sawmill.processors;

import com.google.common.primitives.Doubles;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.Template;
import io.logz.sawmill.TemplateService;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorExecutionException;
import io.logz.sawmill.utilities.JsonUtils;

import javax.inject.Inject;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "math", factory = MathProcessor.Factory.class)
public class MathProcessor implements Processor {
    private final String targetField;
    private final Template expressionTemplate;
    private final ScriptEngine engine;

    public MathProcessor(String targetField, Template expressionTemplate) {
        this.targetField = requireNonNull(targetField, "target field cannot be null");
        this.expressionTemplate = requireNonNull(expressionTemplate, "expression cannot be null");
        ScriptEngineManager engineManager = new ScriptEngineManager();
        engine = engineManager.getEngineByName("JavaScript");
    }

    @Override
    public ProcessResult process(Doc doc) {
        Double result;
        String expression = expressionTemplate.render(doc);

        if (!expression.matches("[0-9+\\-*\\/\\(\\)\\^. ]+")) {
            return ProcessResult.failure("invalid expression");
        }

        try {
            result = Doubles.tryParse(engine.eval(expression).toString());
        } catch (ScriptException e) {
            return ProcessResult.failure("failed to run expression",
                    new ProcessorExecutionException("math", e));
        }

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
}

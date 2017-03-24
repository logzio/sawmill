package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.Template;
import io.logz.sawmill.TemplateService;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.exceptions.SawmillException;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

@ProcessorProvider(type = "addField", factory = AddFieldProcessor.Factory.class, services = TemplateService.class)
public class AddFieldProcessor implements Processor {
    private final Template path;
    private final Optional<Object> value;
    private final Optional<Template> templateValue;

    public AddFieldProcessor(Template path, Optional<Object> value, Optional<Template> templateValue) {
        this.path = checkNotNull(path, "path cannot be null");
        this.value = value;
        this.templateValue = templateValue;
    }

    @Override
    public ProcessResult process(Doc doc) {
        doc.addField(path.render(doc), value.orElse(templateValue.get().render(doc)));
        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        private final TemplateService templateService;
        
        public Factory(TemplateService templateService) {
            this.templateService = templateService;
        }

        @Override
        public Processor create(Map<String,Object> config) {
            AddFieldProcessor.Configuration addFieldConfig = JsonUtils.fromJsonMap(AddFieldProcessor.Configuration.class, config);

            Template path = templateService.createTemplate(addFieldConfig.getPath());
            Optional<Object> value = Optional.of(addFieldConfig.getValue());
            Optional<Template> templateValue = Optional.empty();

            if (value.get() instanceof String) {
                templateValue = Optional.of(templateService.createTemplate((String) value.get()));
                value = Optional.empty();
            } else if (!value.get().getClass().isPrimitive()) {
                throw new ProcessorConfigurationException(String.format("addField does not support [%s]. support primitive types only", value.get().getClass()));
            }

            return new AddFieldProcessor(path, value, templateValue);
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String path;
        private Object value;

        public Configuration() { }

        public Configuration(String path, Object value) {
            this.path = path;
            this.value = value;
        }

        public String getPath() { return path; }

        public Object getValue() { return value; }
    }
}
